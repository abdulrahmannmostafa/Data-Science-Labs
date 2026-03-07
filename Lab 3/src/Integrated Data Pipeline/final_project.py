"""
Book Market Intelligence Pipeline
==================================
Collects, validates, and analyses book-related data from three sources:

    1. Local SQLite library database  (library.db)
    2. GitHub repository search API
    3. Books to Scrape website         (http://books.toscrape.com)

Outputs
-------
    market_intelligence.db  -- structured SQLite database
    analysis.html           -- interactive HTML dashboard report
    exports/                -- CSV export of every table
    pipeline.log            -- full audit log
"""

import sqlite3
import logging
import time
import os
import requests
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import deque


class BookMarketIntelligence:
    """
    End-to-end data-collection and analysis pipeline for the book market.

    Collects from a local library database, the GitHub search API, and
    books.toscrape.com; validates and stores every record; then produces
    a self-contained interactive HTML dashboard report.
    """

    CATEGORY_TAGS = {
        "Sports_And_Games": "sports-and-games_17",
        "Travel": "travel_2",
        "Mystery": "mystery_3",
        "Science_Fiction": "science-fiction_16",
        "History": "history_32",
        "Childrens": "childrens_11",
        "Philosophy": "philosophy_7",
    }

    RATING_MAP = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5,
    }

    STATUS_SUCCESS = "success"
    STATUS_ERROR = "error"

    # ------------------------------------------------------------------
    # Initialisation & lifecycle
    # ------------------------------------------------------------------

    def __init__(self, db_path="market_intelligence.db", plots_path="plots"):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("pipeline.log"),
                logging.StreamHandler(),
            ],
        )

        if os.path.exists(db_path):
            os.remove(db_path)

        self.logger = logging.getLogger("BookMarket")
        self.db_path = db_path
        self.plots_path = plots_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "BookMarketIntelligence/1.0 (Educational)"}
        )

        self.base_url = "http://books.toscrape.com"
        self.rate_limiter = deque()
        self.max_requests = 10
        self.time_window = 60.0
        self.progress = {}

        self.logger.info(f"Pipeline initialised — DB: {db_path}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        """Explicitly close the SQLite connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed.")

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS web_books (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                title      TEXT    NOT NULL,
                price      REAL,
                rating     INTEGER,
                in_stock   INTEGER,
                category   TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(title, category)
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS github_repos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT,
                full_name    TEXT,
                stars        INTEGER,
                forks        INTEGER,
                language     TEXT,
                description  TEXT,
                html_url     TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(full_name)
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS library_books (
                book_id          INTEGER PRIMARY KEY,
                title            TEXT,
                author           TEXT,
                genre            TEXT,
                publication_year INTEGER,
                copies_available INTEGER
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type       TEXT,
                records_collected INTEGER,
                status            TEXT,
                error_message     TEXT,
                timestamp         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        self.conn.commit()

    def _log_pipeline_event(
        self, source_type, records_collected, status, error_message=None
    ):
        self.conn.cursor().execute(
            "INSERT INTO pipeline_logs (source_type, records_collected, status, error_message) VALUES (?, ?, ?, ?)",
            (source_type, records_collected, status, error_message),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Rate-limiting & HTTP helpers
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self):
        now = time.time()
        while self.rate_limiter and now - self.rate_limiter[0] > self.time_window:
            self.rate_limiter.popleft()
        if len(self.rate_limiter) >= self.max_requests:
            wait_time = self.time_window - (now - self.rate_limiter[0])
            self.logger.info(f"Rate limit reached — waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
        self.rate_limiter.append(time.time())

    def _check_robots_txt(self, url):
        """Return True when scraping is permitted. Defaults True on any error."""
        from urllib.robotparser import RobotFileParser

        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch(self.session.headers["User-Agent"], url)
        except Exception:
            return True

    def _scrap_with_retry(self, url, max_attempts=3):
        """GET with exponential backoff. Respects Retry-After on 429/503."""
        for attempt in range(1, max_attempts + 1):
            try:
                self._wait_for_rate_limit()
                response = self.session.get(url, timeout=10)
                if response.status_code in (429, 503):
                    retry_after = int(response.headers.get("Retry-After", 2**attempt))
                    self.logger.warning(
                        f"HTTP {response.status_code} — waiting {retry_after}s"
                    )
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.text
            except Exception as e:
                wait_time = 2 ** (attempt - 1)
                self.logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {max_attempts} attempts failed for {url}")
                    return None

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def _validate_web_book_data(self, book):
        if not book.get("title"):
            self.logger.warning("Invalid book: missing title")
            return False
        if not isinstance(book.get("price"), (int, float)) or book["price"] <= 0:
            self.logger.warning(
                f"Invalid price for '{book.get('title')}': {book.get('price')}"
            )
            return False
        if not (1 <= book.get("rating", 0) <= 5):
            self.logger.warning(
                f"Invalid rating for '{book.get('title')}': {book.get('rating')}"
            )
            return False
        if book.get("in_stock") not in (0, 1):
            self.logger.warning(
                f"Invalid in_stock for '{book.get('title')}': {book.get('in_stock')}"
            )
            return False
        if book.get("category") not in self.CATEGORY_TAGS:
            self.logger.warning(
                f"Invalid category for '{book.get('title')}': {book.get('category')}"
            )
            return False
        return True

    def _validate_book_data(self, book):
        if not book.get("title"):
            self.logger.warning("Invalid book: missing title")
            return False
        if not book.get("author"):
            self.logger.warning(f"Missing author for '{book.get('title')}'")
            return False
        if not book.get("genre"):
            self.logger.warning(f"Missing genre for '{book.get('title')}'")
            return False
        if (
            not isinstance(book.get("publication_year"), int)
            or book["publication_year"] <= 0
        ):
            self.logger.warning(f"Invalid publication_year for '{book.get('title')}'")
            return False
        if (
            not isinstance(book.get("copies_available"), int)
            or book["copies_available"] < 0
        ):
            self.logger.warning(f"Invalid copies_available for '{book.get('title')}'")
            return False
        return True

    def _validate_repo_data(self, repo):
        if not repo.get("name"):
            self.logger.warning("Invalid repo: missing name")
            return False
        if not repo.get("full_name"):
            self.logger.warning("Invalid repo: missing full_name")
            return False
        if not isinstance(repo.get("stars"), int) or repo["stars"] < 0:
            self.logger.warning(f"Invalid stars for '{repo.get('full_name')}'")
            return False
        if not isinstance(repo.get("forks"), int) or repo["forks"] < 0:
            self.logger.warning(f"Invalid forks for '{repo.get('full_name')}'")
            return False
        if not repo.get("html_url") or not repo["html_url"].startswith("http"):
            self.logger.warning(f"Invalid html_url for '{repo.get('full_name')}'")
            return False
        return True

    # ------------------------------------------------------------------
    # Data collection  ①  Local database
    # ------------------------------------------------------------------

    def collect_from_database(self, source_db_path="./library.db"):
        """
        Read books from a local SQLite library and insert into library_books.
        """
        self.logger.info(f"Collecting from database: {source_db_path}")
        source_conn = None
        try:
            source_conn = sqlite3.connect(source_db_path)
            df = pd.read_sql_query(
                """
                SELECT  b.book_id,
                        b.title,
                        a.author_name  AS author,
                        b.genre,
                        b.publication_year,
                        b.copies_available
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                """,
                source_conn,
            )
        except Exception as e:
            self._log_pipeline_event("database", 0, self.STATUS_ERROR, str(e))
            self.logger.error(f"Error reading source database: {e}")
            return pd.DataFrame()
        finally:
            if source_conn:
                source_conn.close()

        try:
            df_valid = df[
                df.apply(lambda row: self._validate_book_data(row.to_dict()), axis=1)
            ].drop_duplicates(subset="book_id", keep="first")

            if not df_valid.empty:
                self.conn.cursor().executemany(
                    "INSERT OR IGNORE INTO library_books (book_id, title, author, genre, publication_year, copies_available) VALUES (?, ?, ?, ?, ?, ?)",
                    df_valid[
                        [
                            "book_id",
                            "title",
                            "author",
                            "genre",
                            "publication_year",
                            "copies_available",
                        ]
                    ].values.tolist(),
                )
                self.conn.commit()

            self._log_pipeline_event("database", len(df_valid), self.STATUS_SUCCESS)
            self.logger.info(f"Collected {len(df_valid)} valid records from database")
            return df_valid

        except Exception as e:
            self._log_pipeline_event("database", 0, self.STATUS_ERROR, str(e))
            self.logger.error(f"Error inserting library data: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Data collection  ②  GitHub API
    # ------------------------------------------------------------------

    def collect_from_api(self, query="books python sports", per_page=20):
        """
        Search GitHub repositories and insert results into github_repos.

        FIX: replaced to_sql(if_exists="append") — which blindly appended rows on
        every run regardless of duplicates — with a temp-table pattern that feeds
        INSERT OR IGNORE so the UNIQUE(full_name) constraint is respected.
        """
        self.logger.info(
            f"Collecting from GitHub API: query='{query}', per_page={per_page}"
        )
        url = "https://api.github.com/search/repositories"
        params = {"q": query, "per_page": per_page, "sort": "stars", "order": "desc"}

        try:
            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 403:
                reset_at = int(
                    response.headers.get("X-RateLimit-Reset", time.time() + 60)
                )
                wait_sec = max(0, reset_at - time.time()) + 1
                self.logger.warning(f"GitHub rate limit — waiting {wait_sec:.0f}s")
                time.sleep(wait_sec)
                response = self.session.get(url, params=params, timeout=10)

            response.raise_for_status()
            items = response.json().get("items", [])

            rows = [
                {
                    "name": item.get("name"),
                    "full_name": item.get("full_name"),
                    "stars": item.get("stargazers_count", 0),
                    "forks": item.get("forks_count", 0),
                    "language": item.get("language"),
                    "description": item.get("description"),
                    "html_url": item.get("html_url"),
                }
                for item in items
            ]

            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset="full_name", keep="first")
            df_valid = df[
                df.apply(lambda row: self._validate_repo_data(row.to_dict()), axis=1)
            ]

            if not df_valid.empty:
                df_valid.to_sql(
                    "github_repos_temp", self.conn, if_exists="replace", index=False
                )
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO github_repos
                        (name, full_name, stars, forks, language, description, html_url)
                    SELECT name, full_name, stars, forks, language, description, html_url
                    FROM github_repos_temp
                    """
                )
                self.conn.execute("DROP TABLE github_repos_temp")
                self.conn.commit()

            self._log_pipeline_event("api", len(df_valid), self.STATUS_SUCCESS)
            self.logger.info(f"Collected {len(df_valid)} valid records from API")
            return df_valid

        except Exception as e:
            self._log_pipeline_event("api", 0, self.STATUS_ERROR, str(e))
            self.logger.error(f"Error collecting from API: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Data collection  ③  Web scraping
    # ------------------------------------------------------------------

    def collect_from_web(
        self, categories=None, resume=False, stop_after=None, max_pages_per_category=5
    ):
        """
        Scrape books from http://books.toscrape.com and insert into web_books.
        Uses a single DB cursor for the entire run.
        Checks robots.txt before every page fetch.
        """
        self.logger.info(
            f"Collecting from web — categories={categories}, resume={resume}, "
            f"stop_after={stop_after}, max_pages_per_category={max_pages_per_category}"
        )

        if categories is None:
            categories = list(self.CATEGORY_TAGS.keys())

        total_collected = 0
        collected_books = []
        cursor = self.conn.cursor()

        for category in categories:
            tag = self.CATEGORY_TAGS.get(category)
            if not tag:
                self.logger.warning(f"Unknown category '{category}' — skipped")
                continue

            category_url = urljoin(
                self.base_url, f"catalogue/category/books/{tag}/index.html"
            )
            page_url = category_url
            page_num = 1

            while page_url and (
                max_pages_per_category is None or page_num <= max_pages_per_category
            ):
                if resume and self.progress.get(category, 0) >= page_num:
                    self.logger.info(f"Resuming '{category}': skipping page {page_num}")
                    page_num += 1
                    continue

                if not self._check_robots_txt(page_url):
                    self.logger.warning(
                        f"Scraping disallowed by robots.txt: {page_url}"
                    )
                    break

                html = self._scrap_with_retry(page_url)
                if not html:
                    break

                soup = BeautifulSoup(html, "html.parser")
                books = soup.select("article.product_pod")
                collected_this_page = 0

                for book in books:
                    try:
                        title = book.h3.a["title"].strip()
                        price_str = book.select_one(".price_color").text.strip()
                        price = float(
                            price_str.replace("£", "")
                            .replace("Â", "")
                            .replace("\xa0", "")
                            .replace(",", "")
                            .strip()
                        )
                        rating_class = book.p.get("class", [])
                        rating_str = next(
                            (c for c in rating_class if c in self.RATING_MAP), None
                        )
                        rating = self.RATING_MAP.get(rating_str, 0)
                        in_stock = (
                            1
                            if "In stock"
                            in book.select_one(".instock.availability").text
                            else 0
                        )

                        book_data = {
                            "title": title,
                            "price": price,
                            "rating": rating,
                            "in_stock": in_stock,
                            "category": category,
                            "scraped_at": datetime.now(),
                        }

                        if self._validate_web_book_data(book_data):
                            cursor.execute(
                                "INSERT OR IGNORE INTO web_books (title, price, rating, in_stock, category, scraped_at) VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    book_data["title"],
                                    book_data["price"],
                                    book_data["rating"],
                                    book_data["in_stock"],
                                    book_data["category"],
                                    book_data["scraped_at"],
                                ),
                            )
                            if cursor.rowcount > 0:
                                collected_books.append(book_data)
                                collected_this_page += 1
                        else:
                            self.logger.warning(
                                f"Invalid book skipped: {book_data['title']}"
                            )

                    except Exception as e:
                        self.logger.warning(f"Error parsing book on {page_url}: {e}")

                self.conn.commit()
                total_collected += collected_this_page
                self.logger.info(
                    f"'{category}' page {page_num}: {collected_this_page} books collected"
                )
                self.progress[category] = page_num

                if stop_after and total_collected >= stop_after:
                    self.logger.info(f"stop_after={stop_after} reached — halting")
                    self._log_pipeline_event(
                        "web", total_collected, self.STATUS_SUCCESS
                    )
                    return pd.DataFrame(collected_books)

                next_link = soup.select_one("li.next a")
                if next_link:
                    page_url = urljoin(page_url, next_link["href"])
                    page_num += 1
                else:
                    break

        self._log_pipeline_event("web", total_collected, self.STATUS_SUCCESS)
        self.logger.info(f"Total web records collected: {total_collected}")
        return pd.DataFrame(collected_books)

    # ------------------------------------------------------------------
    # Analysis — compute all stats (no matplotlib/seaborn)
    # ------------------------------------------------------------------

    def analyze_and_visualize(self):
        """
        Generate statistics and visualizations for all three data sources.
        Returns:
            stats (dict): key statistics and insights
            plots (dict): paths to saved plot images
        """
        os.makedirs(self.plots_path, exist_ok=True)
        conn = self.conn

        # Load tables
        df_library = pd.read_sql("SELECT * FROM library_books", conn)
        df_web = pd.read_sql("SELECT * FROM web_books", conn)
        df_api = pd.read_sql("SELECT * FROM github_repos", conn)

        stats = {}
        plots = {}

        # ---------------- Library Genres ----------------
        if not df_library.empty:
            plt.figure(figsize=(8, 5))
            sns.countplot(
                data=df_library,
                x="genre",
                order=df_library["genre"].value_counts().index,
                color="#3182ce",
            )
            plt.xticks(rotation=45)
            plt.title("Popular Genres in Library Books")
            plt.ylabel("Number of Books")
            plt.xlabel("Genre")
            path = os.path.join(self.plots_path, "popular_genres.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["popular_genres"] = path
            stats["top_genres"] = df_library["genre"].value_counts().to_dict()
            stats["lib_total"] = len(df_library)
        else:
            stats["top_genres"] = {}
            stats["lib_total"] = 0

        # ---------------- Web Price & Rating ----------------
        if not df_web.empty:
            # Stats
            stats["web_total"] = len(df_web)
            stats["price_mean"] = round(float(df_web["price"].mean()), 2)
            stats["price_median"] = round(float(df_web["price"].median()), 2)
            stats["rating_mean"] = round(float(df_web["rating"].mean()), 2)
            stats["rating_counts"] = df_web["rating"].value_counts().to_dict()
            stats["in_stock_pct"] = round(
                float(df_web["in_stock"].sum()) / len(df_web) * 100, 1
            )
            stats["most_expensive_category"] = (
                df_web.groupby("category")["price"].mean().idxmax()
            )
            stats["cheapest_category"] = (
                df_web.groupby("category")["price"].mean().idxmin()
            )
            stats["highest_rated_category"] = (
                df_web.groupby("category")["rating"].mean().idxmax()
            )

            # Price distribution
            plt.figure(figsize=(6, 4))
            sns.histplot(df_web["price"].dropna(), bins=20, kde=True, color="#2b6cb0")
            plt.title("Price Distribution of Web Scraped Books")
            plt.xlabel("Price (£)")
            plt.ylabel("Count")
            path = os.path.join(self.plots_path, "price_distribution.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["price_distribution"] = path

            # Rating distribution
            plt.figure(figsize=(6, 4))
            sns.countplot(
                data=df_web,
                x="rating",
                order=sorted(df_web["rating"].dropna().unique()),
                color="#3182ce",
            )
            plt.title("Distribution of Book Ratings")
            plt.xlabel("Rating")
            plt.ylabel("Count")
            path = os.path.join(self.plots_path, "rating_distribution.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["rating_distribution"] = path

            # Price vs rating
            plt.figure(figsize=(6, 4))
            sns.boxplot(data=df_web, x="rating", y="price", color="#2b6cb0")
            plt.title("Price vs Rating")
            plt.xlabel("Rating")
            plt.ylabel("Price (£)")
            path = os.path.join(self.plots_path, "price_vs_rating.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["price_vs_rating"] = path

        else:
            stats.update(
                {
                    "web_total": 0,
                    "price_mean": 0,
                    "price_median": 0,
                    "rating_mean": 0,
                    "rating_counts": {},
                    "in_stock_pct": 0,
                    "most_expensive_category": "N/A",
                    "cheapest_category": "N/A",
                    "highest_rated_category": "N/A",
                }
            )

        # ---------------- GitHub Languages & Stars/Forks ----------------
        if not df_api.empty:
            top_langs = df_api["language"].dropna().value_counts().head(10).to_dict()
            stats["top_languages"] = top_langs
            stats["api_total"] = len(df_api)
            stats["total_stars"] = int(
                pd.to_numeric(df_api["stars"], errors="coerce").fillna(0).sum()
            )

            # Top languages
            plt.figure(figsize=(7, 4))
            if top_langs:
                sns.barplot(
                    x=list(top_langs.values()),
                    y=list(top_langs.keys()),
                    color="#3182ce",
                )
                plt.xlabel("Number of Repositories")
                plt.ylabel("Language")
                plt.title("Top 10 Programming Languages in GitHub Repositories")
            else:
                plt.text(
                    0.5, 0.5, "No language data available", ha="center", va="center"
                )
                plt.axis("off")
            path = os.path.join(self.plots_path, "top_languages.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["top_languages"] = path

            # Stars vs Forks
            df_nonzero = df_api[(df_api["stars"] > 0) | (df_api["forks"] > 0)]
            plt.figure(figsize=(6, 4))
            if not df_nonzero.empty:
                plt.scatter(df_nonzero["stars"] + 1, df_nonzero["forks"] + 1, alpha=0.7)
                plt.xscale("log")
                plt.yscale("log")
                plt.xlabel("Stars (log scale)")
                plt.ylabel("Forks (log scale)")
                plt.title("Stars vs Forks in GitHub Repositories")
            else:
                plt.text(
                    0.5, 0.5, "No stars/forks data available", ha="center", va="center"
                )
                plt.axis("off")
            path = os.path.join(self.plots_path, "stars_vs_forks.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            plots["stars_vs_forks"] = path

        else:
            stats.update({"top_languages": {}, "api_total": 0, "total_stars": 0})
            for name in ["top_languages.png", "stars_vs_forks.png"]:
                plt.figure()
                plt.text(0.5, 0.5, "No data available", ha="center", va="center")
                plt.axis("off")
                path = os.path.join(self.plots_path, name)
                plt.savefig(path)
                plt.close()
                plots[name.replace(".png", "")] = path

        stats["generated_at"] = datetime.now().strftime("%B %d, %Y at %H:%M")
        return stats, plots

    # ------------------------------------------------------------------
    # HTML report
    # ------------------------------------------------------------------

    def generate_html_report(self, stats, output_file="analysis.html"):
        """
        Generate an HTML report using the analysis stats dictionary.
        All numbers are pulled from the stats dict, not hardcoded.
        """
        html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <title>Book Market Intelligence Report</title>

    <style>
    body {{ font-family: Arial; max-width: 960px; margin: 40px auto; color: #333; line-height: 1.6; }}
    h1 {{ color: #2c5282; border-bottom: 3px solid #2c5282; padding-bottom: 8px; }}
    h2 {{ color: #2b6cb0; margin-top: 36px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
    th, td {{ border: 1px solid #ddd; padding: 10px 14px; text-align: left; }}
    th {{ background: #2c5282; color: #fff; }}
    tr:nth-child(even) {{ background: #f7fafc; }}
    .cards {{ display:flex; flex-wrap:wrap; gap:12px; margin:16px 0; }}
    .card {{ background:#ebf8ff; border-left:4px solid #3182ce; padding:14px 22px; border-radius:4px; min-width:160px; }}
    .card b {{ display:block; font-size:1.4em; color:#2c5282; }}
    img {{ max-width:100%; border-radius:6px; box-shadow:0 2px 10px rgba(0,0,0,.15); margin-top:20px; }}
    ul li {{ margin-bottom: 6px; }}
    footer {{ color: #aaa; font-size: .85em; margin-top:48px; border-top:1px solid #eee; padding-top:12px; }}
    </style>
    </head>

    <body>

    <h1>Book Market Intelligence Report</h1>
    <p>Generated: <strong>{stats.get("generated_at","")}</strong></p>

    <h2>Executive Summary</h2>
    <div class="cards">
    <div class="card"><b>{stats.get("web_total",0)}</b> Web Books Scraped</div>
    <div class="card"><b>{stats.get("api_total",0)}</b> GitHub Repositories</div>
    <div class="card"><b>{stats.get("lib_total",0)}</b> Library Books (DB)</div>
    <div class="card"><b>£{stats.get("price_mean",0)}</b> Average Web Price</div>
    <div class="card"><b>{stats.get("rating_mean",0)} ★</b> Average Rating</div>
    <div class="card"><b>{stats.get("in_stock_pct",0)}%</b> In Stock</div>
    </div>

    <h2>Data Collection Statistics</h2>
    <table>
    <tr><th>Source</th><th>Records Collected</th></tr>
    <tr><td>Library Database</td><td>{stats.get("lib_total",0)}</td></tr>
    <tr><td>Web Scraping</td><td>{stats.get("web_total",0)}</td></tr>
    <tr><td>GitHub API</td><td>{stats.get("api_total",0)}</td></tr>
    </table>

    <h2>Market Insights</h2>
    <table>
    <tr><th>Metric</th><th>Finding</th></tr>
    <tr><td>Most expensive category</td><td><strong>{stats.get("most_expensive_category","N/A")}</strong></td></tr>
    <tr><td>Cheapest category</td><td><strong>{stats.get("cheapest_category","N/A")}</strong></td></tr>
    <tr><td>Highest rated category</td><td><strong>{stats.get("highest_rated_category","N/A")}</strong></td></tr>
    <tr><td>Overall average price</td><td>£{stats.get("price_mean",0)}</td></tr>
    <tr><td>Overall average rating</td><td>{stats.get("rating_mean",0)} / 5</td></tr>
    <tr><td>Availability</td><td>{stats.get("in_stock_pct",0)}% of books currently in stock</td></tr>
    </table>

    <h2>Visualizations</h2>
    <img src="plots/market_analysis.png" alt="Market Analysis Charts">
    <img src="plots/popular_genres.png" alt="Popular Genres">
    <img src="plots/price_vs_rating.png" alt="Price vs Rating">
    <img src="plots/rating_distribution.png" alt="Rating Distribution">
    <img src="plots/stars_vs_forks.png" alt="GitHub Stars vs Forks">
    <img src="plots/top_languages.png" alt="Top Programming Languages">

    <h2>Recommendations</h2>
    <ul>
    <li>Maintain high stock for top-rated categories.</li>
    <li>Leverage popular technologies identified in GitHub repositories.</li>
    <li>Combine web prices and library demand for smarter stocking.</li>
    </ul>

    <footer>Book Market Intelligence System — Data Science Lab 03 — 2026</footer>
    </body>
    </html>
    """

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)
        self.logger.info(f"Report generated at {output_file}")

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_all_data(self, output_dir="exports"):
        """Export every pipeline table to a CSV file under output_dir."""
        os.makedirs(output_dir, exist_ok=True)
        for table in ("web_books", "github_repos", "library_books", "pipeline_logs"):
            df = pd.read_sql_query(f"SELECT * FROM {table}", self.conn)
            path = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(path, index=False, encoding="utf-8")
            print(f"  ✓ {path}  ({len(df)} rows)")
        self.logger.info(f"All data exported to '{output_dir}/'")

    # ------------------------------------------------------------------
    # Pipeline entrypoint
    # ------------------------------------------------------------------

    def run(self, library_db="./library.db", github_query="books python sports"):
        """
        Execute the full pipeline end-to-end.

        Steps
        -----
        1. Collect from local library database
        2. Collect from GitHub API
        3. Collect from books.toscrape.com
        4. Analyse data and compute statistics
        5. Generate interactive HTML report
        6. Export all tables to CSV
        """

        print("=" * 70)
        print("  BOOK MARKET INTELLIGENCE SYSTEM")
        print("=" * 70)
        print("Starting full data collection pipeline...\n")

        print(f"(1/7) Collecting from database: {library_db} ...")
        self.collect_from_database(library_db)

        print(f"\n(2/7) Collecting from GitHub API (query='{github_query}') ...")
        self.collect_from_api(github_query)

        print("\n(3/7) Collecting from web (max 5 pages/category) ...")
        self.collect_from_web(max_pages_per_category=5)

        print("\n(5/7) Analysis and generate plots ...")
        stats, plots = self.analyze_and_visualize()

        print("\n(6/7) Generating HTML report ...")
        self.generate_html_report(stats)

        print("\n(7/7) Exporting all tables to CSV ...")
        self.export_all_data()

        print("\n" + "=" * 70)
        print("Pipeline completed successfully!\n")

        print("Key Statistics:")
        for key, value in stats.items():
            if not isinstance(value, (list, dict)):
                print(f"  {key.replace('_',' ').title():32s}: {value}")

        print("\nOutput Files:")
        print("  analysis.html           — interactive HTML dashboard")
        print("  exports/                — CSV exports of every table")
        print("  pipeline.log            — full audit log")
        print("  market_intelligence.db  — structured SQLite database")
        print("=" * 70)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

if __name__ == "__main__":
    with BookMarketIntelligence() as pipeline:
        pipeline.run(library_db=os.path.join(os.getcwd(), "library.db"))

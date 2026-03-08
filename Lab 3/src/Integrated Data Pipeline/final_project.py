import os
import time
import sqlite3
import logging
import requests
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


from string import Template
from collections import deque
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser


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


class BookMarketIntelligence:
    """
    End-to-end data-collection and analysis pipeline for the book market.

    - Collects from a local library database, the GitHub search API, and books.toscrape.com
    - Validates and stores every record
    - Produces an interactive HTML dashboard report
    """

    def __init__(self, db_path="market_intelligence.db", plots_path="plots"):
        """Constructor initialises logging, database connection, and HTTP session"""

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

        self.base_url = "http://books.toscrape.com"
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "BookMarketIntelligence/1.0 (Educational)"}
        )

        self.rate_limiter = deque()
        self.max_requests = 10
        self.time_window = 60.0

        self.progress = {}

        self.logger.info(f"Pipeline initialised — DB: {db_path}")

    def __enter__(self):
        """Context manager entry to allow cleanup of resources with 'with' statement"""

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit to ensure database connection is closed"""

        self.close()
        return False

    def close(self):
        """Closing the SQLite connection"""

        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")

    def _create_tables(self):
        """Create necessary tables for storing collected data and logs"""

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
        """Insert a log entry into the pipeline_logs table"""

        self.conn.cursor().execute(
            "INSERT INTO pipeline_logs (source_type, records_collected, status, error_message) VALUES (?, ?, ?, ?)",
            (source_type, records_collected, status, error_message),
        )
        self.conn.commit()

    def _wait_for_rate_limit(self):
        """Rate limiter to ensure we don't exceed max_requests within time_window seconds"""

        now = time.time()

        while self.rate_limiter and now - self.rate_limiter[0] > self.time_window:
            self.rate_limiter.popleft()

        if len(self.rate_limiter) >= self.max_requests:
            wait_time = self.time_window - (now - self.rate_limiter[0])
            self.logger.info(f"Rate limit reached — waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

        self.rate_limiter.append(time.time())

    def _check_robots_txt(self, url):
        """
        - Check if scraping the given URL is allowed by robots.txt
        - Caches results for efficiency
        """

        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch(self.session.headers["User-Agent"], url)
        except Exception:
            return True

    def _fetch_with_retry(self, url, retries=3):
        """Scrape a URL with retries and exponential backoff for handling errors and rate limits"""

        # Try up to retries times with exponential backoff
        for attempt in range(1, retries + 1):
            try:
                self._wait_for_rate_limit()
                response = self.session.get(url, timeout=10)

                # Handle HTTP 429 Too Many Requests and 503 Service Unavailable
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
                # Backoff time doubles with each attempt
                wait_time = 2 ** (attempt - 1)
                self.logger.warning(f"Attempt {attempt} failed for {url}: {e}")

                if attempt < retries:
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {retries} attempts failed for {url}")
                    return None

    def _validate_web_book(self, book):
        """Validate scraped book data before insertion into the database"""

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
        if book.get("category") not in CATEGORY_TAGS:
            self.logger.warning(
                f"Invalid category for '{book.get('title')}': {book.get('category')}"
            )
            return False
        return True

    def _validate_library_record(self, book):
        """Validate book data from the library database before insertion"""

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
        """Validate GitHub repository data before insertion into the database"""
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

    def collect_from_database(self, source_db_path="./library.db"):
        """Read books from a local SQLite library and insert into library_books"""

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
            self._log_pipeline_event("database", 0, STATUS_ERROR, str(e))
            self.logger.error(f"Error reading source database: {e}")
            return pd.DataFrame()
        finally:
            if source_conn:
                source_conn.close()

        try:
            df_valid = df[
                df.apply(
                    lambda row: self._validate_library_record(row.to_dict()), axis=1
                )
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

            self._log_pipeline_event("database", len(df_valid), STATUS_SUCCESS)
            self.logger.info(f"Collected {len(df_valid)} valid records from database")
            return df_valid

        except Exception as e:
            self._log_pipeline_event("database", 0, STATUS_ERROR, str(e))
            self.logger.error(f"Error inserting library data: {e}")
            return pd.DataFrame()

    def collect_from_api(self, query="books python sports", per_page=20):
        """Search GitHub repositories and insert results into github_repos"""

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

            self._log_pipeline_event("api", len(df_valid), STATUS_SUCCESS)
            self.logger.info(f"Collected {len(df_valid)} valid records from API")
            return df_valid

        except Exception as e:
            self._log_pipeline_event("api", 0, STATUS_ERROR, str(e))
            self.logger.error(f"Error collecting from API: {e}")
            return pd.DataFrame()

    def collect_from_web(
        self, categories=None, resume=False, stop_after=None, max_pages_per_category=5
    ):
        """
        - Scrape books from http://books.toscrape.com and insert into web_books
        - Checks robots.txt before every page fetch
        """
        self.logger.info(
            f"Collecting from web — categories={categories}, resume={resume}, "
            f"stop_after={stop_after}, max_pages_per_category={max_pages_per_category}"
        )

        if categories is None:
            categories = list(CATEGORY_TAGS.keys())

        total_collected = 0
        collected_books = []
        cursor = self.conn.cursor()

        for category in categories:
            tag = CATEGORY_TAGS.get(category)
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

                html = self._fetch_with_retry(page_url)
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
                            (c for c in rating_class if c in RATING_MAP), None
                        )
                        rating = RATING_MAP.get(rating_str, 0)
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

                        if self._validate_web_book(book_data):
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
                    self._log_pipeline_event("web", total_collected, STATUS_SUCCESS)
                    return pd.DataFrame(collected_books)

                next_link = soup.select_one("li.next a")
                if next_link:
                    page_url = urljoin(page_url, next_link["href"])
                    page_num += 1
                else:
                    break

        self._log_pipeline_event("web", total_collected, STATUS_SUCCESS)
        self.logger.info(f"Total web records collected: {total_collected}")
        return pd.DataFrame(collected_books)

    def analyze_and_visualize(self):
        """Generate statistics and visualizations for all three data sources"""

        os.makedirs(self.plots_path, exist_ok=True)
        conn = self.conn

        # Load tables
        df_library = pd.read_sql("SELECT * FROM library_books", conn)
        df_web = pd.read_sql("SELECT * FROM web_books", conn)
        df_api = pd.read_sql("SELECT * FROM github_repos", conn)

        stats = {}
        plots = {}

        # Library Categories plot, and stats
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

        # Web Price & Rating plots, and stats
        if not df_web.empty:
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

            # Price distribution plots, and stats
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

            # Rating distribution plots, and stats
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

            # Price vs rating plots, and stats
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

            # Market Analysis plots, and stats
            plt.figure(figsize=(8, 5))

            summary_data = {
                "Avg Price (£)": stats["price_mean"],
                "Median Price (£)": stats["price_median"],
                "Avg Rating": stats["rating_mean"],
                "In Stock %": stats["in_stock_pct"],
            }

            sns.barplot(
                x=list(summary_data.keys()),
                y=list(summary_data.values()),
                color="#3182ce",
            )

            plt.title("Book Market Overview")
            plt.ylabel("Value")
            plt.xticks(rotation=20)

            path = os.path.join(self.plots_path, "market_analysis.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()

            plots["market_analysis"] = path

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

        # GitHub Languages and Stars/Forks plots, and stats
        if not df_api.empty:
            top_langs = df_api["language"].dropna().value_counts().head(10).to_dict()
            stats["top_languages"] = top_langs
            stats["api_total"] = len(df_api)
            stats["total_stars"] = int(
                pd.to_numeric(df_api["stars"], errors="coerce").fillna(0).sum()
            )

            # Top languages bar plot, and stats
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

            # Stars vs Forks scatter plot, and stats
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

    def generate_html_report(
        self, stats, report_file="templates/report.html", output_file="analysis.html"
    ):
        """Generate an HTML report using the analysis stats dictionary"""

        with open(report_file, "r", encoding="utf-8") as f:
            template = f.read()

        template = Template(template)
        html = template.safe_substitute(**stats)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)
        self.logger.info(f"Report generated at {output_file}")

    def export_all_data(self, output_dir="exports"):
        """Export every pipeline table to a CSV file under output_dir"""

        os.makedirs(output_dir, exist_ok=True)
        for table in ("web_books", "github_repos", "library_books", "pipeline_logs"):
            df = pd.read_sql_query(f"SELECT * FROM {table}", self.conn)
            path = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(path, index=False, encoding="utf-8")
            print(f"  ✓ {path}  ({len(df)} rows)")
        self.logger.info(f"All data exported to '{output_dir}/'")

    def run(self, library_db="./library.db", github_query="books python sports"):
        """
        Execute the full pipeline

        Steps:
        1. Collect from local library database
        2. Collect from GitHub API
        3. Collect from books.toscrape.com
        4. Analyse data and compute statistics
        5. Generate interactive HTML report
        6. Export all tables to CSV
        """

        print("Starting full data collection pipeline\n")

        print(f"(1/6) Database gatherings from: {library_db}")
        self.collect_from_database(library_db)

        print(f"\n(2/6) GitHub API gatherings with (query='{github_query}')")
        self.collect_from_api(github_query)

        print("\n(3/6) Web scraping gatherings")
        self.collect_from_web(max_pages_per_category=5)

        print("\n(4/6) Data analysis and plots generation")
        stats, _ = self.analyze_and_visualize()

        print("\n(5/6) Generating HTML report")
        self.generate_html_report(stats)

        print("\n(6/6) Exporting all tables to CSV format")
        self.export_all_data()

        print("\n" + "%" * 70)
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
        print("%" * 70)


if __name__ == "__main__":
    with BookMarketIntelligence() as pipeline:
        pipeline.run(library_db=os.path.join(os.getcwd(), "library.db"))

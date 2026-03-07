import sqlite3
import logging
import time
import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from fpdf import FPDF

from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import deque


class BookMarketIntelligence:
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

    def __init__(self, db_path="market_intelligence.db"):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("pipeline.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger("BookMarket")

        self.plots_dir = "plots"

        self.db_path = db_path
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
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed.")

    # -------------------------------------------------------------------------
    # Database helpers
    # -------------------------------------------------------------------------

    def _create_tables(self):
        """
        Create database tables if they don't exist.

        Tables:
            web_books      -- books scraped from books.toscrape.com
            github_repos   -- repositories from the GitHub search API
            library_books  -- books from a local library.db (joined with authors)
            pipeline_logs  -- audit log for every pipeline run
        """
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS web_books (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                price       REAL,
                rating      INTEGER,
                in_stock    INTEGER,   -- 1 = in stock, 0 = out of stock
                category    TEXT,
                scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                status            TEXT,         -- 'success' | 'error'
                error_message     TEXT,         -- NULL on success
                timestamp         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.conn.commit()

    def _log_pipeline_event(
        self, source_type, records_collected, status, error_message=None
    ):
        """Insert a pipeline audit record into pipeline_logs."""
        self.conn.cursor().execute(
            """
            INSERT INTO pipeline_logs
                (source_type, records_collected, status, error_message)
                VALUES (?, ?, ?, ?)
            """,
            (source_type, records_collected, status, error_message),
        )
        self.conn.commit()

    # -------------------------------------------------------------------------
    # Rate limiting & HTTP helpers
    # -------------------------------------------------------------------------

    def _wait_for_rate_limit(self):
        """Block until sending another request is within the rate limit."""
        now = time.time()

        while self.rate_limiter and now - self.rate_limiter[0] > self.time_window:
            self.rate_limiter.popleft()

        if len(self.rate_limiter) >= self.max_requests:
            wait_time = self.time_window - (now - self.rate_limiter[0])
            print(f"Rate limit reached — waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

        self.rate_limiter.append(time.time())

    def _check_robots_txt(self, url):
        """
        Return True when scraping *url* is permitted by the site's robots.txt.
        """
        from urllib.robotparser import RobotFileParser

        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch(self.session.headers["User-Agent"], url)
        except Exception:
            # Cannot read robots.txt → no restrictions apply
            return True

    def _scrap_with_retry(self, url, max_attempts=3):
        """
        GET *url* with exponential-backoff retry.

        Returns the response text on success, or None after all attempts fail.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                self._wait_for_rate_limit()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response.text
            except Exception as e:
                wait_time = 2 ** (attempt - 1)
                self.logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < max_attempts:
                    print(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All attempts failed for {url}")
                    return None

    # -------------------------------------------------------------------------
    # Validation helpers
    # -------------------------------------------------------------------------

    def _validate_web_book_data(self, book):
        """
        Validate a book dict scraped from the web.
        """
        if not book.get("title"):
            self.logger.warning("Invalid book data: missing title")
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
        """Validate a library_books row dict."""
        if not book.get("title"):
            self.logger.warning("Invalid book data: missing title")
            return False
        if not book.get("author"):
            self.logger.warning(
                f"Invalid book data: missing author for '{book.get('title')}'"
            )
            return False
        if not book.get("genre"):
            self.logger.warning(
                f"Invalid book data: missing genre for '{book.get('title')}'"
            )
            return False
        if (
            not isinstance(book.get("publication_year"), int)
            or book["publication_year"] <= 0
        ):
            self.logger.warning(
                f"Invalid publication_year for '{book.get('title')}': {book.get('publication_year')}"
            )
            return False
        if (
            not isinstance(book.get("copies_available"), int)
            or book["copies_available"] < 0
        ):
            self.logger.warning(
                f"Invalid copies_available for '{book.get('title')}': {book.get('copies_available')}"
            )
            return False
        return True

    def _validate_repo_data(self, repo):
        """Validate a GitHub repository dict."""
        if not repo.get("name"):
            self.logger.warning("Invalid repository data: missing name")
            return False
        if not repo.get("full_name"):
            self.logger.warning("Invalid repository data: missing full_name")
            return False
        if not isinstance(repo.get("stars"), int) or repo["stars"] < 0:
            self.logger.warning(
                f"Invalid stars for '{repo.get('full_name')}': {repo.get('stars')}"
            )
            return False
        if not isinstance(repo.get("forks"), int) or repo["forks"] < 0:
            self.logger.warning(
                f"Invalid forks for '{repo.get('full_name')}': {repo.get('forks')}"
            )
            return False
        if not repo.get("html_url") or not repo["html_url"].startswith("http"):
            self.logger.warning(
                f"Invalid html_url for '{repo.get('full_name')}': {repo.get('html_url')}"
            )
            return False
        return True

    # -------------------------------------------------------------------------
    # Data collection
    # -------------------------------------------------------------------------

    def collect_from_database(self, source_db_path="./library.db"):
        """
        Read books from a local SQLite library and upsert into library_books.

        Returns:
            DataFrame: Valid rows that were inserted.
        """
        self.logger.info(f"Collecting from database: {source_db_path}")
        try:
            source_conn = sqlite3.connect(source_db_path)
            df = pd.read_sql_query(
                """
                SELECT  b.book_id,
                        b.title,
                        a.author_name AS author,
                        b.genre,
                        b.publication_year,
                        b.copies_available
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                """,
                source_conn,
            )
            source_conn.close()

            df_valid = df[
                df.apply(lambda row: self._validate_book_data(row.to_dict()), axis=1)
            ].drop_duplicates(subset="book_id")

            if not df_valid.empty:
                self.conn.cursor().executemany(
                    """
                    INSERT OR IGNORE INTO library_books
                        (book_id, title, author, genre, publication_year, copies_available)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
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
            self.logger.error(f"Error collecting from database: {e}")
            return pd.DataFrame()

    def collect_from_api(self, query="books python sports", per_page=20):
        """
        Search GitHub repositories and upsert into github_repos.

        Returns:
            DataFrame: Valid rows that were inserted.
        """
        self.logger.info(
            f"Collecting from GitHub API: query='{query}', per_page={per_page}"
        )
        url = "https://api.github.com/search/repositories"
        params = {"q": query, "per_page": per_page, "sort": "stars", "order": "desc"}

        try:
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
            df_valid = df[
                df.apply(lambda row: self._validate_repo_data(row.to_dict()), axis=1)
            ]

            if not df_valid.empty:
                df_valid.to_sql(
                    "github_repos",
                    self.conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                self.conn.commit()

            self._log_pipeline_event("api", len(df_valid), self.STATUS_SUCCESS)
            self.logger.info(f"Collected {len(df_valid)} valid records from API")
            return df_valid

        except Exception as e:
            self._log_pipeline_event("api", 0, self.STATUS_ERROR, str(e))
            self.logger.error(f"Error collecting from API: {e}")
            return pd.DataFrame()

    def collect_from_web(
        self,
        categories=None,
        resume=False,
        stop_after=None,
        max_pages_per_category=5,
    ):
        """
        Scrape books from http://books.toscrape.com and insert into web_books.

        Args:
            categories (list | None): Category names to scrape (default: all).
            resume (bool): Skip pages already recorded in self.progress.
            stop_after (int | None): Hard cap on total records collected.
            max_pages_per_category (int | None): Page cap per category.

        Returns:
            DataFrame: All books collected in this run.
        """
        self.logger.info(
            f"Collecting from web: categories={categories}, resume={resume}, "
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
                self.logger.warning(f"Unknown category '{category}' skipped")
                continue

            category_url = urljoin(
                self.base_url, f"catalogue/category/books/{tag}/index.html"
            )
            page_url = category_url
            page_num = 1

            while page_url and (
                max_pages_per_category is None or page_num <= max_pages_per_category
            ):
                # Resume: skip pages we have already processed
                if resume and self.progress.get(category, 0) >= page_num:
                    self.logger.info(f"Resuming {category}: skipping page {page_num}")
                    # We cannot advance page_url without fetching, so we restart
                    # the category from the beginning and let the scraper skip.
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
                            .replace("\xa0", "")  # non-breaking space
                            .replace(",", "")
                            .strip()
                        )

                        rating_class = book.p.get("class", [])
                        rating_str = next(
                            (cls for cls in rating_class if cls in self.RATING_MAP),
                            None,
                        )
                        rating = self.RATING_MAP.get(rating_str, 0)

                        in_stock_str = book.select_one(
                            ".instock.availability"
                        ).text.strip()
                        in_stock = 1 if "In stock" in in_stock_str else 0

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
                                """
                                INSERT INTO web_books
                                    (title, price, rating, in_stock, category, scraped_at)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    book_data["title"],
                                    book_data["price"],
                                    book_data["rating"],
                                    book_data["in_stock"],
                                    book_data["category"],
                                    book_data["scraped_at"],
                                ),
                            )
                            collected_books.append(book_data)
                            collected_this_page += 1
                        else:
                            self.logger.warning(
                                f"Invalid book data skipped: {book_data['title']}"
                            )
                    except Exception as e:
                        self.logger.warning(f"Error parsing book on {page_url}: {e}")

                self.conn.commit()
                total_collected += collected_this_page
                self.logger.info(
                    f"Collected {collected_this_page} books from {page_url}"
                )

                self.progress[category] = page_num

                if stop_after and total_collected >= stop_after:
                    self.logger.info(f"Stopping after {total_collected} records")
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
        self.logger.info(f"Total collected from web: {total_collected}")
        return pd.DataFrame(collected_books)

    def analyze_and_visualize(self):
        """
        Generate insights and visualizations.
        Returns a dict of statistics and paths to saved plots.

        Returns:
            stats (dict): Key statistics and insights derived from the data
            plots (dict): Paths to generated plot images
        """

        os.makedirs(self.plots_dir, exist_ok=True)

        conn = self.conn
        df_library = pd.read_sql("SELECT * FROM library_books", conn)
        df_web = pd.read_sql("SELECT * FROM web_books", conn)
        df_api = pd.read_sql("SELECT * FROM github_repos", conn)

        plots = {}
        stats = {}

        # -----------------------
        # Popular Genres (Library)
        # -----------------------
        if not df_library.empty:
            plt.figure(figsize=(8, 5))
            ax = sns.countplot(
                data=df_library,
                x="genre",
                order=df_library["genre"].value_counts().index,
            )
            plt.xticks(rotation=45)
            plt.title("Popular Genres in Library Books")
            plt.ylabel("Number of Books")
            plt.xlabel("Genre")
            path = os.path.join(self.plots_dir, "popular_genres.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["popular_genres"] = path
            plt.close()
            stats["top_genres"] = df_library["genre"].value_counts().to_dict()
        else:
            stats["top_genres"] = {}
            print("No library book data available for genre analysis.")

        # -----------------------
        # Price Distribution (Web)
        # -----------------------
        if not df_web.empty:
            plt.figure(figsize=(6, 4))
            sns.histplot(df_web["price"], bins=20, kde=True)
            plt.title("Price Distribution of Web Scraped Books")
            plt.xlabel("Price (£)")
            plt.ylabel("Count")
            path = os.path.join(self.plots_dir, "price_distribution.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["price_distribution"] = path
            plt.close()
            stats["price_mean"] = round(df_web["price"].mean(), 2)
            stats["price_median"] = round(df_web["price"].median(), 2)

            # Rating Patterns
            plt.figure(figsize=(6, 4))
            sns.countplot(
                data=df_web, x="rating", order=sorted(df_web["rating"].unique())
            )
            plt.title("Distribution of Book Ratings")
            plt.xlabel("Rating")
            plt.ylabel("Count")
            path = os.path.join(self.plots_dir, "rating_distribution.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["rating_distribution"] = path
            stats["rating_counts"] = df_web["rating"].value_counts().to_dict()
            plt.close()

            # Price vs Rating
            plt.figure(figsize=(6, 4))
            sns.boxplot(data=df_web, x="rating", y="price")
            plt.title("Price vs Rating")
            plt.xlabel("Rating")
            plt.ylabel("Price (£)")
            path = os.path.join(self.plots_dir, "price_vs_rating.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["price_vs_rating"] = path
            plt.close()
        else:
            stats.update({"price_mean": 0, "price_median": 0, "rating_counts": {}})
            print("No web book data available for price or rating analysis.")

        # -----------------------
        # GitHub Technology Trends
        # -----------------------
        if not df_api.empty:
            top_langs = df_api["language"].value_counts().head(10).to_dict()
            stats["top_languages"] = top_langs

            plt.figure(figsize=(7, 4))
            if top_langs:
                sns.barplot(x=list(top_langs.values()), y=list(top_langs.keys()))
                plt.title("Top 10 Programming Languages in GitHub Repositories")
                plt.xlabel("Number of Repositories")
                plt.ylabel("Language")
            else:
                plt.text(
                    0.5, 0.5, "No language data available", ha="center", va="center"
                )
                plt.axis("off")
            path = os.path.join(self.plots_dir, "top_languages.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["top_languages"] = path
            plt.close()

            # Stars vs Forks
            df_api_nonzero = df_api[(df_api["stars"] > 0) | (df_api["forks"] > 0)]
            plt.figure(figsize=(6, 4))
            if not df_api_nonzero.empty:
                plt.scatter(
                    df_api_nonzero["stars"] + 1, df_api_nonzero["forks"] + 1, alpha=0.7
                )
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
            path = os.path.join(self.plots_dir, "stars_vs_forks.png")
            plt.tight_layout()
            plt.savefig(path)
            plots["stars_vs_forks"] = path
            plt.close()
        else:
            stats["top_languages"] = {}
            print("No GitHub API data available for language or stars/forks analysis.")
            # Create empty placeholder plots
            for name in ["top_languages.png", "stars_vs_forks.png"]:
                path = os.path.join(self.plots_dir, name)
                plt.figure()
                plt.text(0.5, 0.5, "No data available", ha="center", va="center")
                plt.axis("off")
                plt.savefig(path)
                plots[name.replace(".png", "")] = path
                plt.close()

        return stats, plots

    def generate_report(self, stats, plots, output_file="analysis.pdf"):
        """
        Generate a professional PDF report with executive summary, stats, plots, and recommendations.
        """
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()

        # Title
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Market Intelligence Analysis Report", ln=True, align="C")
        pdf.ln(10)

        # -----------------------
        # Executive Summary
        # -----------------------
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Executive Summary", ln=True)
        pdf.set_font("Arial", "", 11)

        # Safely get most common GitHub language
        top_languages = stats.get("top_languages", {})
        if top_languages:
            most_common_lang = max(top_languages, key=top_languages.get)
        else:
            most_common_lang = "N/A"

        summary_text = f"""
    This report provides insights from the collected library books, web scraped books, and GitHub repositories.
    Total genres collected: {len(stats.get('top_genres', {}))}.
    Average web book price: £{stats.get('price_mean', 0)}.
    Most common GitHub language: {most_common_lang}.
        """
        pdf.multi_cell(0, 6, summary_text)
        pdf.ln(5)

        # -----------------------
        # Visualizations
        # -----------------------
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Visualizations", ln=True)
        for key, path in plots.items():
            pdf.set_font("Arial", "B", 10)
            pdf.cell(0, 8, key.replace("_", " ").title(), ln=True)
            epw = pdf.w - 2 * pdf.l_margin  # effective page width
            pdf.image(path, w=epw)
            pdf.ln(5)

        # -----------------------
        # Recommendations
        # -----------------------
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Recommendations", ln=True)
        pdf.set_font("Arial", "", 11)
        recommendations = """
    1. Focus on popular genres for new acquisitions or marketing.
    2. Consider pricing strategy based on rating trends.
    3. Explore open-source technologies trending on GitHub to identify potential digital projects.
    4. Monitor underrepresented genres for niche market opportunities.
        """
        pdf.multi_cell(0, 6, recommendations)

        # Save PDF
        pdf.output(output_file)
        print(f"\n📄 Report generated: {output_file}")

    def export_all_data(self, output_dir="exports"):
        os.makedirs(output_dir, exist_ok=True)
        for table in ["web_books", "github_repos", "library_books", "pipeline_logs"]:
            df = pd.read_sql_query(f"SELECT * FROM {table}", self.conn)
            path = f"{output_dir}/{table}.csv"
            df.to_csv(path, index=False, encoding="utf-8")
            print(f" {path}  ({len(df)} rows)")
        self.logger.info(f"All data exported to {output_dir}/")

    def run(self, library_db="./library.db", github_query="books python sports"):
        """
        Run the full pipeline: database, API, web scraping.
        """
        print("=" * 70)
        print("  BOOK MARKET INTELLIGENCE SYSTEM")
        print("=" * 70)

        print("Starting full data collection pipeline...\n")

        print(f"(1/5) Collecting data from database ({library_db})...")
        df_db = self.collect_from_database(library_db)

        print(f"(2/5) Collecting data from GitHub API (query='{github_query}')...\n")
        df_api = self.collect_from_api(github_query)

        print(f"(3/5) Collecting data from web scraping (max 5 pages/category)...\n")
        df_web = self.collect_from_web(max_pages_per_category=5)

        slots, plots = self.analyze_and_visualize()

        print(f"(4/5) Generating report...\n")
        self.generate_report(slots, plots)

        print(f"(5/5) Exporting all data...\n")
        self.export_all_data()

        print("\n" + "=" * 70)
        print("\nPipeline execution completed successfully!")

        print("\nKey Statistics:")
        for key, value in slots.items():
            print(f" - {key.replace('_', ' ').title()}: {value}")

        print("\nGenerated Plots:")
        for key, path in plots.items():
            print(f" - {key.replace('_', ' ').title()}: {path}")

        print("Output Files:")
        print(f" - Report: analysis.pdf")
        print(f" - Data Exports: exports/ (CSV files for each table)")
        print("\nFor detailed logs, see pipeline.log")

        print("\n" + "=" * 70)


if __name__ == "__main__":
    pipeline = BookMarketIntelligence()
    pipeline.run(library_db=os.path.join(os.getcwd(), "library.db"))

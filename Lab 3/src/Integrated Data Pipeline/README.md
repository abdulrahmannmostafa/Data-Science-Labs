# Book Market Intelligence Pipeline

A Python data pipeline that collects, validates, and analyses book-related data from three sources, then produces **CSV exports** and a **matplotlib/seaborn plot gallery** saved to a local `plots/` folder.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Prerequisites & Installation](#prerequisites--installation)
3. [How to Run](#how-to-run)
4. [Architecture](#architecture)
5. [Data Schema](#data-schema)
6. [Output Files](#output-files)
7. [Configuration](#configuration)

---

## Project Overview

| Source                                  | What is collected                                                  |
| --------------------------------------- | ------------------------------------------------------------------ |
| **Local database** (`library.db`)       | Books joined with authors — genres, publication years, copy counts |
| **GitHub API**                          | Book-related repositories — stars, forks, languages                |
| **Web scraping** (`books.toscrape.com`) | Prices, ratings, stock status across 7 categories                  |

The pipeline validates every record before inserting it into a structured SQLite database, then generates **6 matplotlib/seaborn plots** and exports all tables to CSV.

### Plots generated

| #   | Plot                      | Data source | Type                |
| --- | ------------------------- | ----------- | ------------------- |
| 1   | Popular Genres            | Library     | Count bar chart     |
| 2   | Price Distribution        | Web         | Histogram + KDE     |
| 3   | Rating Distribution       | Web         | Count bar chart     |
| 4   | Price vs Rating           | Web         | Box plot per rating |
| 5   | Top Programming Languages | GitHub      | Horizontal bar      |
| 6   | Stars vs Forks            | GitHub      | Log-scale scatter   |

---

## Prerequisites & Installation

**Python 3.9+** is required.

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Seed the library database

Run the seed script once to create `library.db` with 50 sample books and 9 authors:

```bash
python seed.py
```

The pipeline expects a `library.db` SQLite file in the same directory as `final_project.py`. It must contain:

- A `books` table with columns: `book_id`, `title`, `genre`, `publication_year`, `copies_available`, `author_id`
- An `authors` table with columns: `author_id`, `author_name`

If this file is absent, the database collection step logs an error and skips gracefully — the rest of the pipeline still runs.

> **Note:** Because the seed uses `random`, re-running it generates a fresh dataset each time. The pipeline deletes `market_intelligence.db` on every startup so stale data never accumulates.

---

## How to Run

```bash
cd "Lab 3/Integrated Data Pipeline"
python final_project.py
```

The pipeline runs all steps automatically and prints progress to the terminal. When finished, plots are in `plots/` and CSV exports are in `exports/`.

### Custom library path or GitHub query

```python
from pipeline import BookMarketIntelligence

with BookMarketIntelligence() as pipeline:
    pipeline.run(
        library_db="./path/to/library.db",
        github_query="book recommendation python",
    )
```

### Custom output paths

```python
with BookMarketIntelligence(db_path="custom.db", plots_path="my_plots") as pipeline:
    pipeline.run()
```

### Run individual steps

```python
from pipeline import BookMarketIntelligence

with BookMarketIntelligence() as p:
    p.collect_from_database("./library.db")      # Source 1
    p.collect_from_api("books data science")      # Source 2
    p.collect_from_web(                           # Source 3
        categories=["Mystery", "Travel"],
        max_pages_per_category=2,
    )
    stats, plots = p.analyze_and_visualize()
    p.export_all_data()
```

---

## Architecture

```
Integrated Data Pipeline
└── analysis.html
└── pipeline.py
    └── BookMarketIntelligence
        │
        ├── __init__              Deletes stale DB, creates fresh connection, sets up logging & HTTP session
        ├── __enter__ / __exit__  Context manager — guarantees DB is always closed
        │
        ├── _create_tables         Creates 4 SQLite tables with UNIQUE constraints
        ├── _log_pipeline_event    Writes one audit row to pipeline_logs
        │
        ├── _wait_for_rate_limit   Sliding-window rate limiter (deque of timestamps)
        ├── _check_robots_txt      Checks robots.txt before every scrape request
        ├── _scrap_with_retry      GET with exponential backoff + Retry-After support
        │
        ├── _validate_web_book_data    Validates scraped book dicts
        ├── _validate_book_data        Validates library book dicts
        ├── _validate_repo_data        Validates GitHub repo dicts
        │
        ├── collect_from_database  ── Source 1: reads library.db, inserts valid rows
        ├── collect_from_api       ── Source 2: GitHub search API, inserts valid repos
        ├── collect_from_web       ── Source 3: scrapes books.toscrape.com per category
        │
        ├── analyze_and_visualize  Computes stats dict + generates and saves all 6 plots
        │
        ├── export_all_data        Dumps every table to a CSV file
        └── run                    Orchestrates all pipeline steps
```

### Data flow

```
library.db  ──► collect_from_database ──┐
GitHub API  ──► collect_from_api      ──┼──► market_intelligence.db
Web scrape  ──► collect_from_web      ──┘
                                         │
                                         ▼
                               analyze_and_visualize()
                                         │
                              ┌──────────┴──────────┐
                              ▼                     ▼
                           stats dict           plots/
                        (metrics,             (6 .png files)
                        aggregates)
                              │
                       export_all_data()
                              │
                              ▼
                         exports/ (4 CSVs)
```

### Duplicate prevention

| Table           | Constraint                   | Insert strategy                       |
| --------------- | ---------------------------- | ------------------------------------- |
| `library_books` | `book_id` PRIMARY KEY        | `INSERT OR IGNORE`                    |
| `web_books`     | `UNIQUE(title, category)`    | `INSERT OR IGNORE` + `rowcount` check |
| `github_repos`  | `UNIQUE(full_name)`          | Temp table + `INSERT OR IGNORE`       |
| `pipeline_logs` | none (intentional audit log) | plain `INSERT`                        |

The DB is also deleted and recreated fresh on every `__init__`, so accumulated duplicates from previous runs can never persist.

---

## Data Schema

### `library_books`

| Column             | Type       | Description                             |
| ------------------ | ---------- | --------------------------------------- |
| `book_id`          | INTEGER PK | Original ID from library.db             |
| `title`            | TEXT       | Book title                              |
| `author`           | TEXT       | Author name (joined from authors table) |
| `genre`            | TEXT       | Genre category                          |
| `publication_year` | INTEGER    | Year of publication                     |
| `copies_available` | INTEGER    | Number of copies held                   |

### `web_books`

| Column       | Type       | Description                     |
| ------------ | ---------- | ------------------------------- |
| `id`         | INTEGER PK | Auto-increment                  |
| `title`      | TEXT       | Book title                      |
| `price`      | REAL       | Price in GBP                    |
| `rating`     | INTEGER    | Star rating 1–5                 |
| `in_stock`   | INTEGER    | 1 = in stock, 0 = out of stock  |
| `category`   | TEXT       | One of the 7 scraped categories |
| `scraped_at` | TIMESTAMP  | When the record was scraped     |

### `github_repos`

| Column         | Type       | Description                   |
| -------------- | ---------- | ----------------------------- |
| `id`           | INTEGER PK | Auto-increment                |
| `name`         | TEXT       | Repository name               |
| `full_name`    | TEXT       | `owner/repo` string (UNIQUE)  |
| `stars`        | INTEGER    | GitHub star count             |
| `forks`        | INTEGER    | Fork count                    |
| `language`     | TEXT       | Primary programming language  |
| `description`  | TEXT       | Repository description        |
| `html_url`     | TEXT       | URL to the repository         |
| `collected_at` | TIMESTAMP  | When the record was collected |

### `pipeline_logs`

| Column              | Type       | Description                                 |
| ------------------- | ---------- | ------------------------------------------- |
| `id`                | INTEGER PK | Auto-increment                              |
| `source_type`       | TEXT       | `database` / `api` / `web`                  |
| `records_collected` | INTEGER    | Count of valid records inserted             |
| `status`            | TEXT       | `success` or `error`                        |
| `error_message`     | TEXT       | NULL on success, exception message on error |
| `timestamp`         | TIMESTAMP  | When this run completed                     |

### Entity Relationship Diagram

```
library.db (external)          market_intelligence.db
┌─────────────┐                ┌──────────────────┐
│   books     │                │  library_books   │
│─────────────│                │──────────────────│
│ book_id  PK │──────────────► │ book_id       PK │
│ title       │                │ title            │
│ genre       │           ┌──► │ author           │
│ pub_year    │           │    │ genre            │
│ copies      │           │    │ publication_year │
│ author_id FK│           │    │ copies_available │
└─────────────┘           │    └──────────────────┘
┌─────────────┐           │
│   authors   │           │    ┌──────────────────┐
│─────────────│           │    │   web_books      │
│ author_id PK│───────────┘    │──────────────────│
│ author_name │                │ id            PK │
└─────────────┘                │ title            │
                               │ price            │
                               │ rating           │
                               │ in_stock         │
                               │ category         │
                               │ scraped_at       │
                               └──────────────────┘

                               ┌──────────────────┐
                               │  github_repos    │
                               │──────────────────│
                               │ id            PK │
                               │ name             │
                               │ full_name  UNIQUE│
                               │ stars            │
                               │ forks            │
                               │ language         │
                               │ description      │
                               │ html_url         │
                               │ collected_at     │
                               └──────────────────┘

                               ┌──────────────────┐
                               │  pipeline_logs   │
                               │──────────────────│
                               │ id            PK │
                               │ source_type      │
                               │ records_collected│
                               │ status           │
                               │ error_message    │
                               │ timestamp        │
                               └──────────────────┘
```

---

## Output Files

| File / Folder                   | Description                                        |
| ------------------------------- | -------------------------------------------------- |
| `plots/popular_genres.png`      | Genre distribution of library books                |
| `plots/price_distribution.png`  | Price histogram with KDE of web scraped books      |
| `plots/rating_distribution.png` | Count of books per star rating                     |
| `plots/price_vs_rating.png`     | Box plot of price per rating value                 |
| `plots/top_languages.png`       | Top 10 languages across GitHub repos               |
| `plots/stars_vs_forks.png`      | Log-scale scatter of stars vs forks                |
| `market_intelligence.db`        | Structured SQLite database with all collected data |
| `pipeline.log`                  | Timestamped log of every step, warning, and error  |
| `exports/web_books.csv`         | All scraped books                                  |
| `exports/github_repos.csv`      | All collected repositories                         |
| `exports/library_books.csv`     | All library books                                  |
| `exports/pipeline_logs.csv`     | Full audit log                                     |

---

## Configuration

All tunable parameters are set in `__init__`:

| Parameter      | Default                  | Description                           |
| -------------- | ------------------------ | ------------------------------------- |
| `db_path`      | `market_intelligence.db` | Output SQLite database filename       |
| `plots_path`   | `plots`                  | Directory where plot images are saved |
| `max_requests` | `10`                     | Maximum requests per time window      |
| `time_window`  | `60.0` seconds           | Rate-limiting window                  |

Scraping categories are defined in `CATEGORY_TAGS` at the top of the class. To add or remove categories, update that dictionary:

```python
CATEGORY_TAGS = {
    "Sports_And_Games": "sports-and-games_17",
    "Travel":           "travel_2",
    "Mystery":          "mystery_3",
    "Science_Fiction":  "science-fiction_16",
    "History":          "history_32",
    "Childrens":        "childrens_11",
    "Philosophy":       "philosophy_7",
}
```

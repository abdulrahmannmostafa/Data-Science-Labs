import sqlite3
import random

conn = sqlite3.connect("./library.db")
cur = conn.cursor()

result = cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Existing tables in library.db:", result.fetchall(),"\n")

cur.execute("DROP TABLE IF EXISTS books")
cur.execute("DROP TABLE IF EXISTS authors")

cur.execute(
    """
CREATE TABLE authors (
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    author_name TEXT NOT NULL
)
"""
)

cur.execute(
    """
CREATE TABLE books (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    author_id INTEGER,
    genre TEXT,
    publication_year INTEGER,
    copies_available INTEGER,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
)
"""
)

authors = [
    "J.K. Rowling",
    "J.R.R. Tolkien",
    "Agatha Christie",
    "Stephen King",
    "Isaac Asimov",
    "George Orwell",
    "Mark Twain",
    "Ernest Hemingway",
    "Leo Tolstoy",
]

cur.executemany("INSERT INTO authors (author_name) VALUES (?)", [(a,) for a in authors])

genres = [
    "Fantasy",
    "Mystery",
    "Science Fiction",
    "History",
    "Children's",
    "Philosophy",
]

books = []
for i in range(50):
    title = f"Book {i+1}"
    author_id = random.randint(1, len(authors))
    genre = random.choice(genres)
    year = random.randint(1950, 2023)
    copies = random.randint(0, 20)
    books.append((title, author_id, genre, year, copies))

cur.executemany(
    """
INSERT INTO books (title, author_id, genre, publication_year, copies_available)
VALUES (?, ?, ?, ?, ?)
""",
    books,
)

conn.commit()
conn.close()
print("library.db created with tables 'authors' and 'books' and sample data inserted.")

import sqlite3


def save_user(name, age):
    conn = sqlite3.connect('users.db')
    # We will later mock this connect() function
    # to avoid creating an actual database during testing
    cursor = conn.cursor()
    cursor.execute(f'INSERT INTO users (name, age) VALUES ("{name}", {age})')
    conn.commit()
    conn.close()

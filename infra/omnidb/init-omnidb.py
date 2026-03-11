"""
Seeds the OmniDB SQLite database with the hackathon PostgreSQL connection.
Runs once as an init container after OmniDB is healthy (migrations complete).
Safe to run multiple times — skips if the connection already exists.
"""

import sqlite3

DB_PATH = "/etc/omnidb/omnidb.db"

conn = sqlite3.connect(DB_PATH)

existing = conn.execute(
    "SELECT id FROM OmniDB_app_connection WHERE alias = 'hackathon'"
).fetchone()

if existing:
    print("OmniDB connection already configured — skipping.")
else:
    conn.execute("""
        INSERT INTO OmniDB_app_connection
            (user_id, alias, conn_string, database, password, port, server,
             ssh_key, ssh_password, ssh_port, ssh_server, ssh_user,
             use_tunnel, technology_id, username, public)
        VALUES (1, 'hackathon', '', 'hackathon', 'hackathon', '5432', 'postgres',
                '', '', '', '', '', 0, 1, 'hackathon', 0)
    """)
    conn.commit()
    print("OmniDB connection configured: hackathon @ postgres:5432/hackathon")

conn.close()

#!/bin/bash
# Cleanup script: keeps only the last 100 logs in the SQLite database.
# Runs daily at midnight via cron.

DB_PATH="/home/telegram/Telegram-Client/backend/data/app.db"

if [ ! -f "$DB_PATH" ]; then
    echo "Database not found at $DB_PATH"
    exit 1
fi

# Delete all rows except the last 100 (by highest id)
python3 -c "
import sqlite3
conn = sqlite3.connect('$DB_PATH')
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM logs')
total = c.fetchone()[0]
if total > 100:
    c.execute('DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 100)')
    conn.commit()
    deleted = total - 100
    print(f'Deleted {deleted} old logs, kept latest 100.')
else:
    print(f'Only {total} logs present, nothing to delete.')
conn.close()
"

# Reclaim disk space
python3 -c "import sqlite3; sqlite3.connect('$DB_PATH').execute('VACUUM'); print('VACUUM complete.')"

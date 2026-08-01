import os
import aiosqlite
from typing import List, Dict, Any, Optional
import contextlib
from .config import DEFAULT_API_ID, DEFAULT_API_HASH

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "app.db")

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT UNIQUE,
            session_name TEXT UNIQUE NOT NULL,
            server_group INTEGER NOT NULL CHECK (server_group IN (1, 2)),
            status TEXT NOT NULL DEFAULT 'RESTING', -- ACTIVE, TYPING, RESTING, FLOOD_WAIT, ERROR, DISABLED
            api_id INTEGER,
            api_hash TEXT,
            flood_until INTEGER DEFAULT 0,
            last_message_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            category TEXT DEFAULT 'general',
            is_active INTEGER DEFAULT 1
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            name TEXT,
            is_active INTEGER DEFAULT 1
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            account_phone TEXT,
            server_group INTEGER,
            action TEXT NOT NULL,
            target TEXT,
            status TEXT NOT NULL, -- SUCCESS, WARNING, ERROR, INFO
            details TEXT
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)

        # Insert default settings if not exists
        default_settings = [
            ('rotation_interval_minutes', '10'),
            ('typing_duration_min', '3'),
            ('typing_duration_max', '7'),
            ('message_delay_min', '5'),
            ('message_delay_max', '15'),
            ('current_active_server', '1'),
            ('is_rotator_running', '0'),
            ('shift_started_at', '0'),
            ('default_api_id', str(DEFAULT_API_ID)),
            ('default_api_hash', DEFAULT_API_HASH)
        ]
        for key, val in default_settings:
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, val))

        await db.commit()



@contextlib.asynccontextmanager
async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db

async def get_setting(key: str, default: str = "") -> str:
    async with get_db() as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row["value"] if row else default

async def set_setting(key: str, value: str):
    async with get_db() as db:
        await db.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?", (key, value, value))
        await db.commit()

async def add_log(action: str, status: str, details: str = "", account_phone: str = None, server_group: int = None, target: str = None):
    async with get_db() as db:
        await db.execute("""
            INSERT INTO logs (action, status, details, account_phone, server_group, target)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (action, status, details, account_phone, server_group, target))
        await db.commit()

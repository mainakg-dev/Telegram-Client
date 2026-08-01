import os
from dotenv import load_dotenv

load_dotenv()

# Default Telegram API credentials (used when accounts don't specify custom ones)
DEFAULT_API_ID = int(os.getenv("DEFAULT_API_ID", "39865871"))
DEFAULT_API_HASH = os.getenv("DEFAULT_API_HASH", "2cc8fee74c199b9a912140e6e6c2e85e")

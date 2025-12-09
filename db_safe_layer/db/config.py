# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/dbname")

settings = Settings()

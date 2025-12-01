# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:62Hx74MV27IRYubc@db.ozobxpasfgenwxrlqbex.supabase.co:5432/postgres")

settings = Settings()

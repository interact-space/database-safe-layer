import contextlib
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DatabaseManager:
    def __init__(self, db_url: str, echo: bool = False):
        """
        Initialize the database connection manager.
        :param db_url: Database connection string (e.g., 'postgresql://user:pass@localhost/dbname')
        :param echo: Whether to print SQL logs (for debugging).
        """
        self.db_url = db_url
        
        # 1. Create Engine
        # check_same_thread=False Only required for SQLite, other databases usually automatically ignore or do not need it
        connect_args = {"check_same_thread": False} if "sqlite" in db_url else {}
        
        self.engine = create_engine(
            self.db_url, 
            echo=echo, 
            connect_args=connect_args,
            pool_pre_ping=True, # Automatically detect and reconnect broken connections
            pool_recycle=3600   # Recycle the connection once every hour to prevent MySQL connection timeout
        )

        #2. Create a session factory (Session Factory)
        # scoped_session ensures thread safety, and each thread obtains an independent session
        self.session_factory = scoped_session(
            sessionmaker(
                autocommit=False, 
                autoflush=False, 
                bind=self.engine
            )
        )

    @contextlib.contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Provides a transaction context manager to automatically handle commit/rollback and close
        Usage:
        with db.session() as s:
            s.add(obj)
        """
        session: Session = self.session_factory()
        try:
            yield session
            session.commit() # No error reported, automatically submitted
        except Exception:
            session.rollback()  # If an error is reported, rollback automatically
            raise
        finally:
            session.close()  # Anyway, close the connection

    def create_tables(self):
        """Create all tables based on models in Base"""
        Base.metadata.create_all(bind=self.engine)

    def drop_tables(self):
        """Delete all tables (use with caution)"""
        Base.metadata.drop_all(bind=self.engine)
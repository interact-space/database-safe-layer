import contextlib
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.ext.declarative import declarative_base

# 创建基础模型类（所有表模型都要继承它）
Base = declarative_base()

class DatabaseManager:
    def __init__(self, db_url: str, echo: bool = False):
        """
        初始化数据库连接管理器
        :param db_url: 数据库连接字符串 (e.g. 'postgresql://user:pass@localhost/dbname')
        :param echo: 是否打印 SQL 日志 (调试用)
        """
        self.db_url = db_url
        
        # 1. 创建引擎 (Engine)
        # check_same_thread=False 仅针对 SQLite 需要，其他数据库通常会自动忽略或不需要
        connect_args = {"check_same_thread": False} if "sqlite" in db_url else {}
        
        self.engine = create_engine(
            self.db_url, 
            echo=echo, 
            connect_args=connect_args,
            pool_pre_ping=True, # 自动检测并重连断开的连接
            pool_recycle=3600   # 1小时回收一次连接，防止 MySQL 连接超时
        )

        # 2. 创建会话工厂 (Session Factory)
        # scoped_session 保证线程安全，每个线程获取独立的 session
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
        提供一个事务上下文管理器，自动处理 commit/rollback 和 close
        用法:
        with db.session() as s:
            s.add(obj)
        """
        session: Session = self.session_factory()
        try:
            yield session
            session.commit()  # 没报错，自动提交
        except Exception:
            session.rollback()  # 报错了，自动回滚
            raise
        finally:
            session.close()   # 无论如何，关闭连接

    def create_tables(self):
        """根据 Base 中的模型创建所有表"""
        Base.metadata.create_all(bind=self.engine)

    def drop_tables(self):
        """删除所有表（慎用）"""
        Base.metadata.drop_all(bind=self.engine)
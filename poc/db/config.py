# config.py
import os
from dotenv import load_dotenv

# 1. 加载 .env 文件中的变量到系统环境变量中
# 如果 .env 文件不存在（例如在生产环境通常直接设置系统变量），这行代码也不会报错
load_dotenv()

class Settings:
    # 2. 读取环境变量，如果没有读取到，则使用后面的默认值
    # 这样即使没配置 .env，代码也能跑起来（例如跑个默认的 SQLite 测试）
    DB_URL = os.getenv("DATABASE_URL", "sqlite:///./test_default.db")
    
    # 你还可以在这里放其他的配置，比如:
    # SECRET_KEY = os.getenv("SECRET_KEY")
    # DEBUG = os.getenv("DEBUG") == "True"

settings = Settings()

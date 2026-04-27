import sys
from pathlib import Path

# Add prefect/app to path for imports
BASE_DIR = Path("D:/!!COLEGE SAUCE/Semester 4/IPBD/IPBD-Teori(2)")
sys.path.insert(0, str(BASE_DIR / "prefect_app" / "app"))

from sqlalchemy import create_engine, text
from settings import settings


def get_db_connection():
    connection_string = settings.get_db_connection_string()
    return create_engine(connection_string)


def init_database():
    with get_db_connection().connect() as connection:
        with open("app/db/schema.sql", "r") as f:
            schema_sql = f.read()
        
        connection.execute(text(schema_sql))
        connection.commit()
    
    print("Database initialized successfully")


if __name__ == "__main__":
    init_database()
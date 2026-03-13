from app.database.postgres import engine
from app.database.schema import metadata

def create_tables():
    metadata.create_all(engine)

if __name__ == "__main__":
    create_tables()
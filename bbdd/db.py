"""Database engine and table creation utilities."""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from conf import *

# Logging configuration
logging.basicConfig(level=logging.WARNING)
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)

# Clear handlers
logging.getLogger('sqlalchemy').handlers = []
logging.getLogger('sqlalchemy.engine').handlers = []
logging.getLogger('sqlalchemy.pool').handlers = []

file_handler = logging.FileHandler('sqlalchemy.log')
file_handler.setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').addHandler(file_handler)

engine = create_engine(f'sqlite:///{DATA_BASE}', echo=True)
Base = declarative_base()


def create_tables(delete_db: bool = False) -> None:
    """Create the database schema.

    Parameters
    ----------
    delete_db:
        If ``True``, existing tables are dropped before being recreated.

    This helper ensures the SQLite database contains all tables defined
    in the ORM models. Errors are logged but not raised to the caller.
    """
    try:
        if delete_db:
            Base.metadata.drop_all(engine)
            logging.info("Tables dropped successfully in the database.")
        Base.metadata.create_all(engine)
        logging.info("Tables created successfully in the database.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        logging.error(traceback.format_exc())

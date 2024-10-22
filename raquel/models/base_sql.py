from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs


class BaseSQL(DeclarativeBase):
    """Main metadata object for all SQLAlchemy models."""
    pass

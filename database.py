from typing import Annotated
from sqlalchemy import String, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase

import config


sync_engine = create_engine(
    url=config.settings.DATABASE_URL_psycopg,
    echo=False,
    # pool_size = 5,
    # max_overflow=10
)

async_engine = create_async_engine(
    url=config.settings.DATABASE_URL_asyncpg,
    echo=True,
)

session_factory = sessionmaker(bind=sync_engine)

async_session_factory = async_sessionmaker(bind=async_engine)


str_255 = Annotated[str, 255]


class Base(DeclarativeBase):
    type_annotation_map = {
        str_255: String(255),
    }
    # can now use this type in supclasses like
    # name: Mapped[str_255]
    ...

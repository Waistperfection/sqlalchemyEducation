from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

import config


sync_engine = create_engine(
    url=config.settings.DATABASE_URL_psycopg,
    echo=True,
    # pool_size = 5,
    # max_overflow=10
)

async_engine = create_async_engine(
    url=config.settings.DATABASE_URL_asyncpg,
    echo=True,
)

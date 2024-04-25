import asyncio
from models import metadata_obj
from sqlalchemy import text, insert
from database import sync_engine, async_engine
from models import workers_table


def sync_execute():
    with sync_engine.connect() as conn:  # use begin() to autocommit after __exit__ from inner scope
        res = conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
        print(f"{res.first()=}")  # can use all(), fetchall(), first()
        conn.commit()


async def async_execute():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
        print(f"{res.all()=}")
        await conn.commit()


def create_tables():
    sync_engine.echo = False
    metadata_obj.drop_all(sync_engine)
    metadata_obj.create_all(sync_engine)
    sync_engine.echo = True


def raw_insert_data():
    with sync_engine.connect() as conn:
        # stmt = create, update, delete
        # query = read
        # stmt = """INSERT INTO workers (username) VALUES
        #     ('bober kurwa'),
        #     ('volk yake bydlo')
        #     """
        # conn.execute(text(stmt))
        stmt = insert(workers_table).values((dict(username="bober kurwa"), dict(username="volk ya perdole")))
        conn.execute(stmt)
        conn.commit()


if __name__ == "__main__":
    # print("sync")
    # sync_execute()
    # print("sync_stopped")
    # print("\n" * 2)
    # print("async")
    # asyncio.run(async_execute())
    # print("async stopped")
    pass

import asyncio
from sqlalchemy import Integer, and_, func, text, insert, select, update

from database import sync_engine, async_engine
from models import workers_table, resumes_table, metadata_obj
from .test_data import resumes


class SyncCore:

    @staticmethod
    def create_tables():
        sync_engine.echo = False
        metadata_obj.drop_all(sync_engine)
        metadata_obj.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def sync_execute():
        with sync_engine.connect() as conn:  # use begin() to autocommit after __exit__ from inner scope
            res = conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
            print(f"{res.first()=}")  # can use all(), fetchall(), first()
            conn.commit()

    @staticmethod
    def insert_data():
        with sync_engine.connect() as conn:
            # stmt = create, update, delete
            # query = read
            # stmt = """INSERT INTO workers (username) VALUES
            #     ('bober kurwa'),
            #     ('volk yake bydlo')
            #     """
            # conn.execute(text(stmt))
            stmt = insert(workers_table).values(
                (dict(username="bober kurwa"), dict(username="volk ya perdole"))
            )
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(workers_table)
            result = conn.execute(query)
            workers = result.all()  # first() one() one_or_none()
            print(workers)

    @staticmethod
    def update_worker(worker_id: int = 1, new_username: str = "aisha"):
        with sync_engine.connect() as conn:

            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)

            stmt = (
                update(workers_table).values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                # .filter(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )

            conn.execute(stmt)
            conn.commit()
    
    @staticmethod
    def join_cte_subquery_window_func(like_language="python"):
        """
        WITH helper2 as (
        SELECT *, compensation - avg_workload_compensation as avg_diff from (SELECT
            w.id,
            w.username,
            r.compensation,
            r.workload,
            avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
        FROM resumes as r
        INNER JOIN workers as w on r.worker_id = w.id) as helper1)
        SELECT * FROM helper2
        ORDER BY avg_diff DESC
        """
        r = resumes_table.alias()
        w = workers_table.alias()
        subq = (
            select(
                r,
                w,
                func.avg(r.c.compensation)
                .over(partition_by=r.c.workload)
                .cast(Integer)
                .label("avg_workload_compensation"),
            )
            # .select_from(r) # not working here!!!
            .join(
                r, r.c.worker_id == w.c.id
            ).subquery(  # on_default - INNER JOIN, full=True - FULL JOIN, isouter = True - LEFT JOIN, RIGHT JOIN - not implemented
                "helper1"
            )
        )
        cte = select(
            subq.c.worker_id,
            subq.c.username,
            subq.c.compensation,
            subq.c.workload,
            subq.c.avg_workload_compensation,
            (subq.c.compensation - subq.c.avg_workload_compensation).label("avg_diff"),
        ).cte("helper2")
        query = select(cte).order_by(cte.c.avg_diff.desc())
        # print(query.compile(dialect=sqlalchemy.dialects.mysql.dialect()))
        with sync_engine.connect() as conn:
            result = conn.execute(query)
            result = result.all()
        # print(*result, sep="\n")
        return result


class AsyncCore:
    # Асинхронный вариант, не показанный в видео
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)

    @staticmethod
    async def insert_workers():
        async with async_engine.connect() as conn:
            # stmt = """INSERT INTO workers (username) VALUES
            #     ('Jack'),
            #     ('Michael');"""
            stmt = insert(workers_table).values(
                [
                    {"username": "Jack"},
                    {"username": "Michael"},
                ]
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_workers():
        async with async_engine.connect() as conn:
            query = select(workers_table)  # SELECT * FROM workers
            result = await conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        async with async_engine.connect() as conn:
            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table).values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def insert_resumes():
        async with async_engine.connect() as conn:

            stmt = insert(resumes_table).values(resumes)
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        async with async_engine.connect() as conn:
            query = (
                select(
                    resumes_table.c.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(resumes_table.c.compensation)
                    .cast(Integer)
                    .label("avg_compensation"),
                )
                .select_from(resumes_table)
                .filter(
                    and_(
                        resumes_table.c.title.contains(like_language),
                        resumes_table.c.compensation > 40000,
                    )
                )
                .group_by(resumes_table.c.workload)
                .having(func.avg(resumes_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await conn.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

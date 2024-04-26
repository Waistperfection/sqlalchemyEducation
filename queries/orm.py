from sqlalchemy import Integer, and_, insert, select, func, cast
from database import Base
from database import session_factory, sync_engine, async_engine, async_session_factory
from models import ResumeOrm, WorkerOrm, Workload
from test_data import resumes, additional_resumes, additional_workers


class SyncOrm:

    @staticmethod
    def create_tables():
        sync_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def toggle_echo():
        sync_engine.echo = not sync_engine.echo
        print(f"{sync_engine.echo=}")

    @staticmethod
    def insert_data():
        worker_michel = WorkerOrm(username="Michel")
        worker_john = WorkerOrm(username="John")
        with session_factory() as session:
            # session.add(worker_michel)
            # session.add(worker_john)
            session.add_all([worker_michel, worker_john])
            session.commit()

    @staticmethod
    def insert_resumes():
        with session_factory() as session:
            orm_resumes = [ResumeOrm(**item) for item in resumes]
            session.add_all([*orm_resumes])
            session.commit()

    @staticmethod
    def select_workers():
        with session_factory() as session:
            # worker_id = 1
            # worker_bober = session.get(WorkerOrm, worker_id) # (WorkerOrm, {"id": worker_id}) # returns only one record
            query = select(WorkerOrm)
            result = session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 1, new_username: str = "Michanya"):
        with session_factory() as session:
            worker_michel = session.get(WorkerOrm, worker_id)
            worker_michel.username = new_username
            session.flush()  # to send records to database without commit!
            # session.expire(worker_michel) # to rollback all changes for one object
            # session.expore_all() # to rollback all changes for all not committed objects
            # session.refresh(worker_bober) # refresh object from db
            session.commit()  # commit will close transaction

    @staticmethod
    def select_avg_compensation_for_workload(like_language: str = "Python"):
        """
        SELECT workload, AVG(compensation)::int as avg_compensation FROM resumes # ::int - type casting to int from float
        WHERE title LIKE %python% and compensation > 40000
        GROUP BY workload
        """
        query = (
            select(
                ResumeOrm.workload,
                cast(func.avg(ResumeOrm.compensation), Integer).label(
                    "avg_compensation"
                ),
            )
            .select_from(ResumeOrm)
            .filter(
                and_(
                    ResumeOrm.title.icontains(like_language),
                    ResumeOrm.compensation >= 40000,
                )
            )
            .group_by(ResumeOrm.workload)
            .having(cast(func.avg(ResumeOrm.compensation), Integer) > 70000)
        )
        # print(query.compile(compile_kwargs={"literal_binds": True}))
        with session_factory() as session:
            result = session.execute(query)
            result = result.all()
            print(result)

    @staticmethod
    def insert_additional_workers_with_resumes():
        insert(WorkerOrm).values(additional_workers)
        insert(ResumeOrm).values(additional_resumes)


class AsyncOrm:

    # Асинхронный вариант, не показанный в видео
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

    @staticmethod
    async def insert_workers():
        async with async_session_factory() as session:
            worker_jack = WorkerOrm(username="Jack")
            worker_michael = WorkerOrm(username="Michael")
            session.add_all([worker_jack, worker_michael])
            # flush взаимодействует с БД, поэтому пишем await
            await session.flush()
            await session.commit()

    @staticmethod
    async def select_workers():
        async with async_session_factory() as session:
            query = select(WorkerOrm)
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        async with async_session_factory() as session:
            worker_michael = await session.get(WorkerOrm, worker_id)
            worker_michael.username = new_username
            await session.refresh(worker_michael)
            await session.commit()

    @staticmethod
    async def insert_resumes():
        async with async_session_factory() as session:
            orm_resumes = [ResumeOrm(**item) for item in resumes]
            session.add_all([*orm_resumes])
            await session.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        async with async_session_factory() as session:
            query = (
                select(
                    ResumeOrm.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(ResumeOrm.compensation)
                    .cast(Integer)
                    .label("avg_compensation"),
                )
                .select_from(ResumeOrm)
                .filter(
                    and_(
                        ResumeOrm.title.contains(like_language),
                        ResumeOrm.compensation > 40000,
                    )
                )
                .group_by(ResumeOrm.workload)
                .having(func.avg(ResumeOrm.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await session.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

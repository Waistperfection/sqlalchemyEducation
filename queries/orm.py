from sqlalchemy import Integer, and_, insert, select, func, cast
from sqlalchemy.orm import aliased, joinedload, selectinload, contains_eager
from database import Base
from database import session_factory, sync_engine, async_engine, async_session_factory
from models import ResumeOrm, VacancyOrm, WorkerOrm, Workload
from .test_data import resumes, additional_resumes, additional_workers


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
        with session_factory() as session:
            insert_workers = insert(WorkerOrm).values(additional_workers)
            insert_resumes = insert(ResumeOrm).values(additional_resumes)
            session.execute(insert_workers)
            session.execute(insert_resumes)
            session.commit()
        print("addition insertion ended")

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
        r = aliased(ResumeOrm)
        w = aliased(WorkerOrm)
        subq = (
            select(
                r,
                w,
                func.avg(r.compensation)
                .over(partition_by=r.workload)
                .cast(Integer)
                .label("avg_workload_compensation"),
            )
            # .select_from(r) # not working here!!!
            .join(
                r, r.worker_id == w.id
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
        with session_factory() as session:
            result = session.execute(query)
            result = result.all()
        print(*result, sep="\n")

    @staticmethod
    def select_workers_with_lazy_relationship():  # lazyload - not good for all relations
        query = select(WorkerOrm)
        with session_factory() as session:
            res = session.execute(query)
            result = res.scalars().all()  # UNIQUE
            resume1 = result[0].resumes
            print(resume1)
            resume2 = result[1].resumes
            print(resume2)

    @staticmethod
    def select_workers_with_lazy_joined_relationship():  ## joinedload - good for /many to one/ and /one to one/ relations
        query = select(WorkerOrm).options(
            joinedload(WorkerOrm.resumes)
        )  ## too match same data in traffic
        with session_factory() as session:
            res = session.execute(query)
            result = res.unique().scalars().all()
            resume1 = result[0].resumes
            print(resume1)
            resume2 = result[1].resumes
            print(resume2)

    @staticmethod
    def select_workers_with_lazy_selectinload_relationship():  ## selectinload - good for /one to many/ and /many to many/ relations.
        query = select(WorkerOrm).options(
            selectinload(WorkerOrm.resumes)
        )  ## it's like prefetch_related, make 2 requests first to mother entities and seconde filtered by first ids
        with session_factory() as session:  ## careful to data traffic between db and app
            res = session.execute(query)
            result = res.unique().scalars().all()
            resume1 = result[0].resumes
            print(resume1)
            resume2 = result[1].resumes
            print(resume2)

    @staticmethod
    def select_workers_with_condition_relationship():
        query = select(WorkerOrm).options(selectinload(WorkerOrm.resumes_parttime))
        with session_factory() as session:
            res = session.execute(query)
            result = res.scalars().all()
        for worker in result:
            print(worker)
            for resume in worker.resumes_parttime:
                print("\t", resume)

    @staticmethod
    def select_workers_with_contains_eager():  # previous relations returns all users, and some users with resumes, but this relations will return only users, with parttime resume
        query = (
            select(WorkerOrm)
            # .join(WorkerOrm.resumes)
            .join(ResumeOrm, WorkerOrm.id == ResumeOrm.worker_id)
            .filter(ResumeOrm.workload == Workload.parttime)
            .options(
                contains_eager(WorkerOrm.resumes)
            )  # contains eager convert table to included objects, using already existing joins!
        )
        with session_factory() as session:
            res = session.execute(query)
            result = res.unique().scalars().all()
        for worker in result:
            print(worker)
            for resume in worker.resumes:
                print("\t", resume)

    @staticmethod
    def select_workers_and_resumes_with_limit():
        subq = (
            select(ResumeOrm.id.label("parttime_resume_id"))
            .filter(ResumeOrm.worker_id == WorkerOrm.id)
            .order_by(WorkerOrm.id.desc(), ResumeOrm.compensation.desc())
            .limit(3)
            .scalar_subquery()  # because it's only one item in request not [(id), (id)] but [id, id]
            .correlate(WorkerOrm)
        )

        query = (
            select(WorkerOrm)
            .join(ResumeOrm, ResumeOrm.id.in_(subq))
            .options(contains_eager(WorkerOrm.resumes))
        )

        with session_factory() as session:
            res = session.execute(query)
            result = res.unique().scalars().all()

        for worker in result:
            print(worker)
            for resume in worker.resumes:
                print("\t", resume)

    @staticmethod
    def add_vacancies_and_replies():
        with session_factory() as session:
            new_vacancy = VacancyOrm(
                title="junior Python Developer", compensation=100000
            )
            resume_1 = session.get(ResumeOrm, 1)
            resume_2 = session.get(ResumeOrm, 2)
            resume_1.vacancies_replied.append(new_vacancy)
            resume_2.vacancies_replied.append(new_vacancy)
            session.commit()

    @staticmethod
    def select_resume_with_all_relationship():
        query = (
            select(ResumeOrm)
            .options(joinedload(ResumeOrm.worker))
            .options(selectinload(ResumeOrm.vacancies_replied))
            # .options(selectinload(ResumeOrm.vacancies_replied).load_only(<fields_names>))
        )
        with session_factory() as session:
            res = session.execute(query)
            result_orm = res.unique().scalars().all()
        print(result_orm)



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

from datetime import datetime, UTC
import enum
from typing import Annotated

from sqlalchemy import (
    ForeignKey,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    text,
    TIMESTAMP,
    Enum,
)
from database import Base
from sqlalchemy.orm import Mapped, mapped_column, relationship

metadata_obj = MetaData()


class Workload(enum.Enum):
    parttime = "parttime"
    fulltime = "fulltime"


########################################3
# DECLARATIVE POWER!!!!
# can generate reusable column


## custom fields aliases
pk_int_field = Annotated[int, mapped_column(primary_key=True)]
created_at_field = Annotated[
    datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))
]
updated_at_field = Annotated[
    datetime,
    mapped_column(
        onupdate=(lambda: datetime.now(UTC)),
        server_default=text("TIMEZONE('utc', now())"),
    ),
]


## Declarative tables
class WorkerOrm(Base):
    __tablename__ = "workers"

    # id: Mapped[int] = mapped_column(primary_key=True)
    id: Mapped[pk_int_field]
    username: Mapped[str] = mapped_column(String(255))

    ##relations
    resumes: Mapped[list["ResumeOrm"]] = relationship(
        back_populates="worker",
        # backref="worker", ## almost same as back_populates but ref in other table create automatically almost deprecated style by dzen of python
        # primarjoin = "workers.id == resumes.worker_id"
    )

    resumes_parttime: Mapped[list["ResumeOrm"]] = relationship(
        back_populates="worker",
        # backref="worker", ## almost same as back_populates but ref in other table create automatically almost deprecated style by dzen of python
        primaryjoin="and_(workers.c.id == resumes.c.worker_id, resumes.c.workload == 'parttime')",  ## to set SQL ON expression, can use sqlalchemy functions and expressions or SQL expressions with text() without import this funcs to
        ## in prymaryjoin can use also ResumeOrm.id style and its will be also good practice
        # lazy = "selectin" ## to set policy with of loading this relation automatically!!! selectin - not good practice
    )


class ResumeOrm(Base):
    __tablename__ = "resumes"

    id: Mapped[pk_int_field]
    # id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255))
    compensation: Mapped[int | None]
    workload: Mapped[Workload]
    # can use WorkerOrm.id but it not good practice because u need to import this class and can catch cycle imports
    worker_id: Mapped[int] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"))
    # call function on server to map now func output to UTC timezone
    # can use default to use python func before create
    created_ad: Mapped[datetime] = mapped_column(
        server_default=text("TIMEZONE('utc', now())")
    )
    # updated_at: Mapped[datetime] = mapped_column(onupdate=(lambda: datetime.now(datetime.UTC)))
    created_at: Mapped[created_at_field]
    updated_at: Mapped[updated_at_field]

    ## relations
    worker: Mapped["WorkerOrm"] = relationship(
        back_populates="resumes",
    )

    # def __repr__(self):
    #     return f"{self.id=}, {self.title=}, {self.compensation=}"


########################################3


########################################3
# imperative style!!!!
workers_table = Table(
    "workers",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("username", String, nullable=False),
)


resumes_table = Table(
    "resumes",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("title", String(256)),
    Column("compensation", Integer, nullable=True),
    Column("workload", Enum(Workload)),
    Column("worker_id", ForeignKey("workers.id", ondelete="CASCADE")),
    Column("created_at", TIMESTAMP, server_default=text("TIMEZONE('utc', now())")),
    Column(
        "updated_at",
        TIMESTAMP,
        server_default=text("TIMEZONE('utc', now())"),
        onupdate=lambda: datetime.now(UTC),
    ),
)
########################################3

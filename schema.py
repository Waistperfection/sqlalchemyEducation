from datetime import datetime
from pydantic import BaseModel

from models import Workload


class WorkerAddDTO(BaseModel):
    username: str


class WorkerDTO(WorkerAddDTO):
    id: int


class ResumeAddDTO(BaseModel):
    title: str
    compensation: int
    workload: Workload
    worker_id: int


class ResumeDTO(ResumeAddDTO):
    id: int
    created_at: datetime
    updated_at: datetime


class ResumeRelDTO(ResumeDTO):
    worker: "WorkerDTO"


class WorkersRelDTO(WorkerDTO):
    resumes: list["ResumeDTO"]

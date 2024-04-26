from models import Workload

resumes = [
    {
        "title": "Python Junior Developer",
        "compensation": 50000,
        "workload": Workload.fulltime,
        "worker_id": 1,
    },
    {
        "title": "Python Разработчик",
        "compensation": 150000,
        "workload": Workload.fulltime,
        "worker_id": 1,
    },
    {
        "title": "Python Data Engineer",
        "compensation": 250000,
        "workload": Workload.parttime,
        "worker_id": 2,
    },
    {
        "title": "Data Scientist",
        "compensation": 300000,
        "workload": Workload.fulltime,
        "worker_id": 2,
    },
]

additional_workers = [
    {"username": "Artem"},  # id 3
    {"username": "Roman"},  # id 4
    {"username": "Petr"},  # id 5
]
additional_resumes = [
    {
        "title": "Python программист",
        "compensation": 60000,
        "workload": "fulltime",
        "worker_id": 3,
    },
    {
        "title": "Machine Learning Engineer",
        "compensation": 70000,
        "workload": "parttime",
        "worker_id": 3,
    },
    {
        "title": "Python Data Scientist",
        "compensation": 80000,
        "workload": "parttime",
        "worker_id": 4,
    },
    {
        "title": "Python Analyst",
        "compensation": 90000,
        "workload": "fulltime",
        "worker_id": 4,
    },
    {
        "title": "Python Junior Developer",
        "compensation": 100000,
        "workload": "fulltime",
        "worker_id": 5,
    },
]

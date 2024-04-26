import os

# import sys
# import site
# site.main()

# sys.path.insert(1, os.path.join(sys.path[0], ".."))

from queries.core import SyncCore, AsyncCore
from queries.orm import SyncOrm, AsyncOrm

# SyncCore.create_tables()
# SyncCore.insert_data()
# SyncCore.update_worker(worker_id=2, new_username="pipidaster where are you")
# SyncCore.select_workers()

SyncOrm.create_tables()
SyncOrm.toggle_echo()
SyncOrm.insert_data()
SyncOrm.update_worker()
SyncOrm.insert_resumes()
SyncOrm.select_workers()
# SyncOrm.toggle_echo()
# SyncOrm.select_avg_compensation_for_workload()

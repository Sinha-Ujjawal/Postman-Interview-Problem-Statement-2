from dataclasses import dataclass
from datetime import timedelta
import logging
from logging import Logger
from typing import Iterable, List, Tuple
from toolz import partition_all
import prefect
from prefect import Flow, task
from db_helpers import DBCreds
from api import get_catgory_apis, CategoryApi
from db import refresh_stg_category_apis

# configuring sqlalchemy logger
sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
sqlalchemy_logger.setLevel(logging.INFO)

# remove existing handlers
for handler in sqlalchemy_logger.handlers:
    sqlalchemy_logger.removeHandler(handler)

sqlalchemy_logger.propagate = False
# now if you use logger it will not log to console.

# create file handler that logs debug and higher level messages
fh = logging.FileHandler("sqlalchemy_core.log", mode="w")
fh.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s | %(message)s")
fh.setFormatter(formatter)

# add the handlers to logger
sqlalchemy_logger.addHandler(fh)
##


@dataclass(frozen=True)
class FlowParameters:
    db_creds: DBCreds
    data_insert_chunksize: int = 100


# tasks


@task(max_retries=3, retry_delay=timedelta(minutes=3))
def refresh_stg_category_apis_taskfn(chunksize: int, db_creds: DBCreds):
    logger: Logger = prefect.context.get("logger")

    def data_generator() -> Iterable[List[Tuple[str, str]]]:
        catapis: List[CategoryApi]
        for catapis in partition_all(chunksize, get_catgory_apis()):
            items = [(catapi.category, catapi.api) for catapi in catapis]
            yield items

    refresh_stg_category_apis(
        catapis=data_generator(),
        db_creds=db_creds,
        log_progress=logger.info,
    )


##


def create_flow(flow_params: FlowParameters, flow_name: str) -> Flow:
    with Flow(name=flow_name) as flow:
        refresh_stg_category_apis_taskfn(
            chunksize=flow_params.data_insert_chunksize, db_creds=flow_params.db_creds
        )
    return flow

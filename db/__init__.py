from typing import Callable, Iterable, List, Tuple, Optional
import sqlalchemy as sa
from db_helpers import DBCreds, ensure_tables, truncate_table
from db.tables import stg_category_apis

MYSQL_ENGINE = "mysql+mysqldb"


def refresh_stg_category_apis(
    *,
    catapis: Iterable[List[Tuple[str, str]]],
    db_creds: DBCreds,
    log_progress: Optional[Callable[[str], None]] = None,
):
    db_engine = db_creds.create_db_connection(engine=MYSQL_ENGINE)
    ensure_tables([stg_category_apis], db_engine=db_engine)

    with db_engine.begin() as conn:
        truncate_table(stg_category_apis, conn)
        for items in catapis:
            appending_catapis_to_stg_category_apis(items, conn)
            if log_progress is not None:
                log_progress(f"Bulk inserted {len(items)} rows in stg_category_apis")


def appending_catapis_to_stg_category_apis(
    items: List[Tuple[str, str]], conn: sa.engine.Connection
):
    conn.execute(
        stg_category_apis.insert(),
        [{"category": category, "api": api} for category, api in items],
    )

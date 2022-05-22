from typing import Callable, Iterable, List, Tuple, Optional
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from db_helpers import DBCreds, ensure_tables, truncate_table
from db.tables import stg_category_apis, dwh_categories, dwh_apis

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


def update_dwh_categories(db_creds: DBCreds):
    db_engine = db_creds.create_db_connection(engine=MYSQL_ENGINE)
    ensure_tables([stg_category_apis, dwh_categories], db_engine=db_engine)

    insert_stmt = mysql.insert(dwh_categories).from_select(
        ["category"],
        sa.select([sa.func.distinct(stg_category_apis.columns["category"])]),
    )

    do_update_stmt = insert_stmt.on_duplicate_key_update(
        category=insert_stmt.inserted["category"],
    )

    with db_engine.begin() as conn:
        conn.execute(do_update_stmt)


def update_dwh_apis(db_creds: DBCreds):
    db_engine = db_creds.create_db_connection(engine=MYSQL_ENGINE)
    ensure_tables([stg_category_apis, dwh_categories, dwh_apis], db_engine=db_engine)

    insert_stmt = mysql.insert(dwh_apis).from_select(
        ["category_id", "api"],
        sa.select(
            [
                dwh_categories.columns["id"].label("category_id"),
                stg_category_apis.columns["api"],
            ]
        ).select_from(
            stg_category_apis.join(
                dwh_categories,
                onclause=(
                    stg_category_apis.columns["category"]
                    == dwh_categories.columns["category"]
                ),
            )
        ),
    )

    do_update_stmt = insert_stmt.on_duplicate_key_update(
        api=insert_stmt.inserted["api"],
    )

    with db_engine.begin() as conn:
        conn.execute(do_update_stmt)

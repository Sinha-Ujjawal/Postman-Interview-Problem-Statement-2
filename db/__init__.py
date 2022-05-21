from typing import List, Tuple
from db_helpers import DBCreds, ensure_tables
from db.tables import stg_category_apis

MYSQL_ENGINE = "mysql+mysqldb"


def appending_to_stg_category_apis(catapis: List[Tuple[str, str]], dbcreds: DBCreds):
    db_engine = dbcreds.create_db_connection(engine=MYSQL_ENGINE)
    ensure_tables([stg_category_apis], db_engine=db_engine)
    # TODO write logic for appending data to stg_category_apis table

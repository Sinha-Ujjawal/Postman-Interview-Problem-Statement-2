from typing import List
from dataclasses import dataclass
import sqlalchemy as sa


@dataclass(frozen=True)
class DBCreds:
    username: str
    password: str
    host: str
    port: int

    def create_db_connection(self, engine: str, **extra_kwargs) -> sa.engine.Engine:
        db_uri = f"{engine}://{self.username}:{self.password}@{self.host}:{self.port}"
        return sa.create_engine(db_uri, **extra_kwargs)


def truncate_table(table: sa.Table, conn: sa.engine.Connection):
    conn.execute(f"TRUNCATE TABLE {table.schema}.{table.name};")


def ensure_tables(tables: List[sa.Table], db_engine: sa.engine.Engine):
    for table in tables:
        table.metadata.create_all(db_engine, tables=[table], checkfirst=True)

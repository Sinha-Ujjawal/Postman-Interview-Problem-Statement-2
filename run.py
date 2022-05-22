from prefect import Flow
import toml
from prefect.executors import LocalDaskExecutor
from db_helpers import DBCreds
from flow import FlowParameters, create_flow


def db_creds_from_toml(toml_file: str) -> DBCreds:
    toml_contents = toml.load(toml_file)
    return DBCreds(
        username=toml_contents["USERNAME"],
        password=toml_contents["PASSWORD"],
        host=toml_contents["HOST"],
        port=toml_contents["PORT"],
    )


def create_flow_for_main() -> Flow:
    db_creds = db_creds_from_toml("./db.toml")
    flow_params = FlowParameters(db_creds=db_creds, data_insert_chunksize=23)
    return create_flow(flow_params=flow_params, flow_name="Flow: Category Apis")


def main():
    flow = create_flow_for_main()
    _ = flow.run(executor=LocalDaskExecutor())


if __name__ == "__main__":
    main()

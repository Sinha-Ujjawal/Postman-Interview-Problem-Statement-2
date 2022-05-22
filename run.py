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


def main():
    db_creds = db_creds_from_toml("./db.toml")
    flow_params = FlowParameters(db_creds=db_creds, data_insert_chunksize=23)
    flow = create_flow(flow_params=flow_params, flow_name="Flow: Category Apis")
    _ = flow.run(executor=LocalDaskExecutor())


if __name__ == "__main__":
    main()

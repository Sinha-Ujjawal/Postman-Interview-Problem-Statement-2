from dataclasses import dataclass
from prefect import Flow
from db_helpers import DBCreds


@dataclass(frozen=True)
class FlowParameters:
    db_creds: DBCreds


def create_flow(flow_params: FlowParameters, flow_name: str) -> Flow:
    with Flow(name=flow_name) as flow:
        pass
    return flow

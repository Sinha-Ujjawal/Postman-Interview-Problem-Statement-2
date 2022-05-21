import toml
from db_helpers import DBCreds


def db_creds_from_toml(toml_file: str) -> DBCreds:
    toml_contents = toml.load(toml_file)
    return DBCreds(
        username=toml_contents["USERNAME"],
        password=toml_contents["PASSWORD"],
        host=toml_contents["HOST"],
        port=toml_contents["PORT"],
    )


def main():
    pass


if __name__ == "__main__":
    main()

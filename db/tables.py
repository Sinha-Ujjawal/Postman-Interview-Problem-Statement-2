import sqlalchemy as sa

metadata = sa.MetaData(schema="github_api_repo")

stg_category_apis = sa.Table(
    "stg_category_apis",
    metadata,
    sa.Column("category", sa.VARCHAR(128), nullable=False),
    sa.Column("api", sa.VARCHAR(128), nullable=False),
)

dwh_categories = sa.Table(
    "categories",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("category", sa.VARCHAR(128), nullable=False),
    sa.Column("created_at", sa.TIMESTAMP, default=sa.func.current_timestamp()),
)

dwh_apis = sa.Table(
    "apis",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("category_id", sa.ForeignKey("categories.id")),
    sa.Column("api", sa.VARCHAR(128), nullable=False),
    sa.Column("created_at", sa.TIMESTAMP, default=sa.func.current_timestamp()),
)

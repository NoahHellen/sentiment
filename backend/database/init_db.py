"""Create the schema against the database configured in `.env`.

Run with:  python -m database.init_db  [--drop]

`create_all` is idempotent (it checks for existing tables first), so this is
safe to re-run. It is deliberately not a full migration tool: the schema is
still young enough that recreating it is cheaper than versioning it. Swapping in
Alembic later is a drop-in change, since the models are the source of truth.
"""

import argparse
import sys

from sqlalchemy import inspect

from database.base import Base
from database.session import engine

# Importing the models registers them on Base.metadata; without this the
# create_all below would silently create nothing.
from database import models  # noqa: F401  (side-effecting import)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drop",
        action="store_true",
        help="drop the known tables before creating them (destroys all data)",
    )
    args = parser.parse_args()

    print(f"target: {engine.url.host}/{engine.url.database}")

    if args.drop:
        confirm = input("this deletes all rows in batches and items. type 'drop' to confirm: ")
        if confirm.strip() != "drop":
            print("aborted")
            return 1
        Base.metadata.drop_all(bind=engine)
        print("dropped existing tables")

    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    for table in Base.metadata.sorted_tables:
        columns = inspector.get_columns(table.name)
        indexes = inspector.get_indexes(table.name)
        print(f"\n{table.name}  ({len(columns)} columns)")
        for column in columns:
            nullable = "" if column["nullable"] else " not null"
            print(f"    {column['name']:<18} {column['type']}{nullable}")
        for index in indexes:
            kind = "unique" if index["unique"] else "index"
            print(f"    [{kind}] {index['name']}: {', '.join(index['column_names'])}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

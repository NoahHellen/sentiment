# Sentiment

FastAPI service, SQLAlchemy models, and the worker pipeline.

See the [repository README](../README.md) for the design, API reference, configuration and
deployment notes.

```bash
uv sync
uv run python -m database.init_db      # create the schema
uv run uvicorn api.main:app --reload   # API + in-process worker
uv run python -m services.run_worker   # worker only, if run separately
```

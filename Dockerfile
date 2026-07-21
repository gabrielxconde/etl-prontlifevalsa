FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY . .

CMD ["sh", "-c", "cd src && uv run python raw_ingest.py && cd ../valsa && uv run dbt deps --profiles-dir .. && uv run dbt run --profiles-dir .."]
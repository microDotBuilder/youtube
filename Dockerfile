# Use Python 3.12 slim as base image
FROM python:3.12-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy requirements files
COPY requirements.txt .
COPY pyproject.toml .

# Install dependencies using uv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a directory for credentials
RUN mkdir -p /app/credentials

# Command to run the application
CMD ["uv", "run", "analyze_trending.py"] 
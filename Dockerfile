FROM python:3.12-slim

WORKDIR /app

# Install system dependencies needed by pandas/numpy/scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (includes boto3 for S3)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

RUN mkdir -p data && chmod +x mlb \
    scripts/run_postgame.sh \
    scripts/run_pregame.sh \
    scripts/run_starters.sh

ENTRYPOINT ["/bin/bash"]
CMD ["scripts/run_postgame.sh"]

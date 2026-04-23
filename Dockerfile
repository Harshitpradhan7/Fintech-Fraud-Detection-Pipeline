# Use an official Spark-Python image
FROM bitnami/spark:3.5.0

# Set working directory
WORKDIR /app

# Install dependencies as Root
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy our source code
COPY . /app

# Set Python Path to include our src directory
ENV PYTHONPATH="/app"

# Command to run the orchestrator
CMD ["python", "main.py"]

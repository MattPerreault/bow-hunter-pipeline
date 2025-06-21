# Use a specific Python version that is compatible with your packages
FROM python:3.12-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Set environment variables if needed
ENV PYTHONPATH=/app/src

# Command to run your application (can be overridden by docker-compose.yaml)
CMD ["python", "src/data_collection/ingest_population_data.py"] 
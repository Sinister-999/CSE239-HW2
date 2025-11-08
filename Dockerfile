FROM python:3.9-slim

# Install required packages
RUN pip install rpyc

# Create working directory
WORKDIR /app

# Copy Python files
COPY *.py /app/

# Create txt directory for data
RUN mkdir -p /app/txt

# Expose the port for RPyC
EXPOSE 18861

# Default command (will be overridden in docker-compose)
CMD ["python", "worker.py"]
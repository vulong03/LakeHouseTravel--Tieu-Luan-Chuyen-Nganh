FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /app/

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app_forecast.py /app/

# Expose Gradio port
EXPOSE 7860

# Environment variables
ENV PYTHONUNBUFFERED=1

# Run Gradio app
CMD ["python", "app_forecast.py"]

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

# Copy both applications
COPY app_forecast.py /app/
COPY app_lstm_volume.py /app/

# Expose Gradio ports
EXPOSE 7860
EXPOSE 7861

# Environment variables
ENV PYTHONUNBUFFERED=1

# Default: run the new LSTM dashboard
# Override with: docker run -e APP=old to run app_forecast.py
ARG APP=lstm
ENV APP=${APP}

CMD sh -c "if [ '$APP' = 'old' ]; then python app_forecast.py; else python app_lstm_volume.py; fi"

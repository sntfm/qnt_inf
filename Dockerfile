FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir streamlit pandas requests

# Copy app directory
COPY app/ ./app/

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app/st_latency_hist.py", "--server.port=8501", "--server.address=0.0.0.0"]


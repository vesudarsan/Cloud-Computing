FROM python:3.11-slim
WORKDIR /app

# Create the config directory inside the container
RUN mkdir -p /app/config
RUN mkdir -p /app/data

COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

# Expose Flask port
EXPOSE 6001

# Prevent output buffering
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /app/citibike_project

WORKDIR /app/citibike_project
EXPOSE 6789
CMD ["mage", "start", "--host", "0.0.0.0", "--port", "6789"]

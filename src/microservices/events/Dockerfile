FROM python:3.13-slim

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y gcc python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
	pip install "poetry==2.0.1" && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-directory

CMD ["python", "app.py"]
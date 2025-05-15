FROM --platform=linux/amd64 python:3.13-slim

WORKDIR /app
COPY . /app

RUN pip install "poetry==2.0.1" && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-directory

CMD ["python", "app.py"]
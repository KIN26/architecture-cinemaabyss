FROM python:3.13-alpine

WORKDIR /app
COPY . /app

RUN pip install "poetry==2.0.1" && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --no-directory

CMD ["python", "app.py"]
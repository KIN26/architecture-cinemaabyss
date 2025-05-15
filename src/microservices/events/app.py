import uvicorn
import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from functools import lru_cache

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response, JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)




class AppSettings(BaseSettings):
    port: int = 8081
    host: str = "0.0.0.0"
    reload: bool = True
    kafka_brokers: str = "localhost:9092"
    payment_topic: str = "payment-events"
    movie_topic: str = "movie-events"
    user_topic: str = "user-events"

    model_config = SettingsConfigDict(populate_by_name=True, from_attributes=True, env_file=".env", extra="ignore")


class Movie(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int | None = None
    rating: float | None = None
    genres: list[str] | None = None
    description: str | None = None


class User(BaseModel):
    user_id: int
    username: str | None = None
    email: str | None = None
    action: str
    timestamp: datetime


class Payment(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: str | None = None


class Event(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: dict


class CommonResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: Event


settings = AppSettings()

@lru_cache
def producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)

@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    topics = [settings.user_topic, settings.payment_topic, settings.movie_topic]

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_brokers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    try:
        await consumer.start()
        await producer().start()
        asyncio.create_task(consume(consumer))
        yield
    finally:
        await consumer.stop()
        await producer().stop()


app = FastAPI(lifespan=lifespan)

async def consume(consumer: AIOKafkaConsumer) -> None:
    async for msg in consumer:
        logger.info("Received message from topic %s: %s", msg.topic, msg.value)


@app.get("/api/events/health")
async def heath() -> Response:
    return JSONResponse(jsonable_encoder({"status": True}))

async def _do_logic(entity_name: str, topic: str, entity_id: int, id_postfix: str, payload: BaseModel,) -> CommonResponse:
    event_id = f"{entity_name}-{entity_id}-{id_postfix}"
    event = Event(id=event_id, type=entity_name, timestamp=datetime.now(), payload=payload.model_dump(exclude_none=True))
    result = await producer().send_and_wait(topic, value=event.model_dump_json().encode("utf-8"))

    return CommonResponse(
        status="success",
        partition=result.partition,
        offset=result.offset,
        event=event,
    )

@app.post("/api/events/movie", response_model=CommonResponse, status_code=201)
async def movie(payload: Movie) -> CommonResponse:
    return await _do_logic("movie", settings.movie_topic, payload.movie_id, payload.action, payload)

@app.post("/api/events/user", response_model=CommonResponse, status_code=201)
async def user(payload: User) -> CommonResponse:
    return await _do_logic("user", settings.user_topic, payload.user_id, payload.action, payload)

@app.post("/api/events/payment", response_model=CommonResponse, status_code=201)
async def payment(payload: Payment) -> CommonResponse:
    return await _do_logic("payment", settings.payment_topic, payload.payment_id, payload.status, payload)


if __name__ == '__main__':
    uvicorn.run("app:app", host=settings.host, port=settings.port, reload=settings.reload)


from random import randint

import uvicorn
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import Response
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    port: int = 8081
    host: str = "0.0.0.0"
    reload: bool = True
    monolith_url: str
    movies_service_url: str
    events_service_url: str
    gradual_migration: bool
    movies_migration_percent: int
    model_config = SettingsConfigDict(populate_by_name=True, from_attributes=True, env_file=".env", extra="ignore")


settings = AppSettings()
client = httpx.AsyncClient()
app = FastAPI()

def choose_backend() -> str:
    """Выбирает URL в зависимости от процента."""
    if randint(1, 100) <= settings.movies_migration_percent:
        return settings.movies_service_url
    return settings.monolith_url


async def make_request(request: Request, url: str):
    return await client.request(
        method=request.method,
        url=url,
        headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
        content=await request.body(),
        params=request.query_params
    )


@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def movies(request: Request) -> Response:
    url = f"{choose_backend()}/api/movies"
    response = await make_request(request, url)
    return Response(content=response.content, status_code=response.status_code, headers=dict(response.headers))


@app.api_route("/health", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def health(request: Request) -> Response:
    url = f"{settings.monolith_url}/health"
    response = await make_request(request, url)
    return Response(content=response.content, status_code=response.status_code, headers=dict(response.headers))


@app.api_route("/api/users", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def users(request: Request) -> Response:
    url = f"{settings.monolith_url}/api/users"
    response = await make_request(request, url)
    return Response(content=response.content, status_code=response.status_code, headers=dict(response.headers))



if __name__ == '__main__':
    uvicorn.run("app:app", host=settings.host, port=settings.port, reload=settings.reload)


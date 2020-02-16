import asyncio
from uuid import uuid4
import json
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Security
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import Response, JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.background import BackgroundTask, BackgroundTasks
from service.routes import control, data, logs, places, services, statuses, subscription, ticket
import configuration as cfg
from service import settings as ws
import nest_asyncio
from fastapi.security.api_key import APIKeyHeader, APIKey
nest_asyncio.apply()


name = 'remote'

API_KEY = "thenb!oronaal_lazo57tathethomenasas"
API_KEY_NAME = "access-token"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

app = FastAPI(title="Remote management Module",
              description="Wisepark Monitoring and Remote Management Module",
              version="0.0.1", debug=cfg.asgi_debug)
app.include_router(control.router)
app.include_router(data.router)
app.include_router(logs.router)
app.include_router(places.router)
app.include_router(services.router)
app.include_router(statuses.router)
app.include_router(subscription.router)
app.include_router(ticket.router)


async def get_api_key(
    api_key_header: str = Security(api_key_header),
):

    if api_key_header == API_KEY:
        return api_key_header
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
        )


@app.on_event('startup')
async def startup():
    ws.logger = await ws.logger.getlogger(cfg.log)
    # await ws.dbconnector_is.connect()
    # await ws.dbconnector_wp.connect()
    # ws.devices = await ws.dbconnector_is.callproc('is_devices_get', rows=-1, values=[None, None, None, None, None])
    # await ws.soapconnector.connect()
    # await ws.amqpconnector.connect()


@app.on_event('shutdown')
async def shutdown():
    await ws.logger.warning({'module': name, 'info': 'Webservice is shutting down'})
    ws.dbconnector_is.pool.close()
    ws.dbconnector_wp.pool.close()
    await ws.dbconnector_is.pool.wait.closed()
    await ws.dbconnector_wp.pool.wait.closed()
    await ws.logger.shutdown()
    await app.logger.shutdown()


@app.get('/')
async def homepage(api_key: APIKey = Depends(get_api_key)):
    return app.description


@app.get('/rdbs')
async def rdbs():
    return {'IS': ws.dbconnector_is.connected, 'WP': ws.dbconnector_wp.connected}


def run():
    uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level='debug')

from enum import Enum
import asyncio
from uuid import uuid4
import json
import uvicorn
from fastapi import FastAPI
from starlette.responses import Response, JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.background import BackgroundTask, BackgroundTasks
from typing import Optional
from service.routes import control, data, logs, places, services, statuses, subscription, ticket
import configuration as cfg
from service import settings as ws
import nest_asyncio
nest_asyncio.apply()

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


name = 'remote'

app.logger = ws.logger


@app.on_event('startup')
async def startup():
    ws.logger = await ws.logger.getlogger(cfg.log)
    await ws.dbconnector_is.connect()
    await ws.dbconnector_wp.connect()
    ws.devices = await ws.dbconnector_is.callproc('is_devices_get', rows=-1, values=[])
    await ws.soapconnector.connect()
    await ws.amqpconnector.connect()


@app.on_event('shutdown')
async def shutdown():
    await ws.logger.warning({'module': name, 'info': 'Webservice is shutting down'})
    ws.dbconnector_is.pool.close()
    ws.dbconnector_wp.pool.close()
    await ws.dbconnector_is.pool.wait.closed()
    await ws.dbconnector_wp.pool.wait.closed()
    # await ws.logger.shutdown()
    # await app.logger.shutdown()


@app.get('/')
async def homepage():
    return app.description


@app.get('/rdbs')
async def rdbs():
    return {'IS': ws.dbconnector_is.connected, 'WP': ws.dbconnector_wp.connected}


def run():
    uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level='debug')

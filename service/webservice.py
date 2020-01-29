from enum import Enum
import logging
import asyncio
from uuid import uuid4
import json
import uvicorn
from fastapi import FastAPI
from starlette.responses import Response, JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.background import BackgroundTask, BackgroundTasks
from typing import Optional
import nest_asyncio
import json
from service.routes import control, data, places, services, subscription, ticket
import configuration as cfg
from service import settings as ws
nest_asyncio.apply()


app = FastAPI(title="Remote management Module",
              description="Wisepark Monitoring and Remote Management Module",
              version="0.0.1", debug=cfg.asgi_debug)
app.include_router(data.router)
app.include_router(places.router)
app.include_router(services.router)
app.include_router(ticket.router)
app.include_router(remote.router)
name = 'remote'


@app.on_event('startup')
async def startup():
    ws.logger = await ws.logger.getlogger(cfg.log)
    app.logger = ws.logger
    await app.logger.warning('Webservice is starting up...')
    await app.logger.info({'info': "Establishing RDBS Integration Pool Connection"})
    await ws.dbconnector_is.connect()
    await app.logger.info({'info': 'Integration RDBS Pool Connection', 'status': ws.dbconnector_is.connected()})
    await ws.dbconnector_wp.connect()
    await app.logger.info({'info': 'Wisepark RDBS Pool Connection', 'status': ws.dbconnector_wp.connected()})
    rc.devices = await cfg.dbconnector_is.callproc('is_devices_get', rows=-1, values=[])
    await app.logger.info({'info': "Establishing SOAP Service Connection"})
    rc.soapconnector = await ws.soapconnector.connect()
    await app.logger.info({'info': 'Wisepark Soap Connection', 'status': ws.soapconnector_wp.connected()})


@app.on_event('shutdown')
async def shutdown():
    await app.logger.warning('Shutting down Webservice')
    cfg.dbconnector_is.pool.close()
    cfg.dbconnector_wp.pool.close()
    await cfg.dbconnector_is.pool.wait.closed()
    await cfg.dbconnector_wp.pool.wait.closed()
    await cfg.logger.shutdown()
    await app.logger.shutdown()


def run():
    uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level='debug')

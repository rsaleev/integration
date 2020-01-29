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
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from typing import Optional
import nest_asyncio
import json
from routes import data, places, services, ticket, remote
import configuration as cfg
from api.control import RemoteControl
nest_asyncio.apply()


app = FastAPI(title="Remote management Module",
              description="Wisepark Monitoring and Remote Management Module",
              version="0.0.1", debug=cfg.asgi_debug)
cfg.dbconnector_is = AsyncDBPool(cfg.is_cnx, None)
cfg.dbconnector_wp = AsyncDBPool(cfg.wp_cnx, None)
rc = remote.RemoteControl()
app.include_router(data.router)
app.include_router(places.router)
app.include_router(services.router)
app.include_router(ticket.router)
app.include_router(remote.router)
name = 'remote'


@app.on_event('startup')
async def startup():
    cfg.logger = await AsyncLogger().getlogger(cfg.log)
    app.logger = cfg.logger
    await app.logger.warning('Webservice is starting up...')
    await app.logger.info({'info': "Establishing RDBS Integration Pool Connection"})
    await cfg.dbconnector_is.connect()
    await app.logger.info({'info': 'Integration RDBS Pool Connection', 'status': cfg.dbconnector_is.connected()})
    await cfg.dbconnector_wp.connect()
    await app.logger.info({'info': 'Wisepark RDBS Pool Connection', 'status': cfg.dbconnector_wp.connected()})
    rc.devices = await cfg.dbconnector_is.callproc('is_devices_get', rows=-1, values=[])


@app.on_event('shutdown')
async def shutdown():
    await app.logger.warning('Shutting down Webservice')
    cfg.dbconnector_is.pool.close()
    cfg.dbconnector_wp.pool.close()
    await cfg.dbconnector_is.pool.wait.closed()
    await cfg.dbconnector_wp.pool.wait.closed()
    await cfg.logger.shutdown()
    await app.logger.shutdown()


c

uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level='debug')

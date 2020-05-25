import hashlib
import asyncio
from uuid import uuid4
import json
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Security, Header
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import Response, JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.background import BackgroundTask, BackgroundTasks
from service.routes import control, data, logs, places, services, devices, subscription, ticket, converters, parkconfig
import configuration as cfg
from service import settings as ws
import nest_asyncio
from fastapi.security.api_key import APIKeyHeader, APIKey, APIKeyCookie
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import secrets
import datetime


name = 'remote'

API_KEY = "thenb!oronaal_lazo57tathethomenasas"
API_KEY_NAME = "token"
TIMESTAMP = "ts"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
ts_key_header = APIKeyHeader(name=TIMESTAMP, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)


async def get_api_key(
    api_key_header: str = Security(api_key_header),
    ts_key_header: str = Security(ts_key_header),
):
    if api_key_header:
        secret = hashlib.sha256((ts_key_header+API_KEY).encode()).hexdigest()
        if api_key_header != secret or int(datetime.now().timestamp()) - int(ts_key_header) > 5:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Unauthorized"
            )
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Unauthorized"
        )


security = HTTPBasic()


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = 'admin'
    correct_password = API_KEY
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


app = FastAPI(title="Remote management Module",
              description="Wisepark Monitoring and Remote Management Module",
              version="0.0.1 BETA 1", debug=True if cfg.asgi_log == 'debug' else False,
              docs_url=None, redoc_url=None, openapi_url=None)

app.include_router(control.router, dependencies=[Depends(get_api_key)])
app.include_router(data.router, dependencies=[Depends(get_api_key)])
app.include_router(logs.router, dependencies=[Depends(get_api_key)])
app.include_router(places.router, dependencies=[Depends(get_api_key)])
app.include_router(services.router, dependencies=[Depends(get_api_key)])
app.include_router(devices.router, dependencies=[Depends(get_api_key)])
app.include_router(subscription.router, dependencies=[Depends(get_api_key)])
app.include_router(ticket.router, dependencies=[Depends(get_api_key)])
app.include_router(converters.router, dependencies=[Depends(get_api_key)])
app.include_router(parkconfig.router, dependencies=[Depends(get_api_key)])


@app.on_event('startup')
async def startup():
    ws.logger = await ws.logger.getlogger(cfg.log)
    await ws.dbconnector_is.connect()
    await ws.dbconnector_wp.connect()
    await ws.soapconnector.connect()
    await ws.amqpconnector.connect()


@app.on_event('shutdown')
async def shutdown():
    await ws.dbconnector_wp.disconnect()
    await ws.dbconnector_is.disconnect()
    await ws.soapconnector.disconnect()
    await ws.amqpconnector.disconnect()
    await ws.logger.warning({'module': name, 'info': 'Webservice is shutting down'})
    await ws.logger.shutdown()


@app.get('/')
async def homepage():
    return {'title': app.title,
            'description': app.description,
            'version': app.version}


@app.get('/status')
async def rdbs():
    return {'Wisepark RDBS Connection': ws.dbconnector_wp.connected,
            'Integration RDBS Connection': ws.dbconnector_is.connected,
            'Wisepark SOAP Connection': ws.soapconnector.connected}


def run():
    uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level=cfg.asgi_log)

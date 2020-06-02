import hashlib
import asyncio
from uuid import uuid4
import json
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Security, Header
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN, HTTP_407_PROXY_AUTHENTICATION_REQUIRED
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import Response, JSONResponse, PlainTextResponse
from starlette.requests import Request
from starlette.background import BackgroundTask, BackgroundTasks
from integration.service.routes import control, data, logs, places, services, devices, subscription, ticket, converters, parkconfig
import configuration.settings as cs
from integration.service import settings as ws
import nest_asyncio
from fastapi.security.api_key import APIKeyHeader, APIKey, APIKeyCookie
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import secrets
from datetime import datetime


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
        secret = hashlib.sha256(f"{ts_key_header}{API_KEY}".encode()).hexdigest()
        if api_key_header != secret or int(datetime.now().timestamp()) - int(ts_key_header) > 5:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Unauthorized"
            )
        else:
            return api_key_header
    else:
        raise HTTPException(
            status_code=HTTP_407_PROXY_AUTHENTICATION_REQUIRED, detail="Authentication required"
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
              version="0.0.2 BETA", debug=True if cs.IS_WEBSERVICE_LOG_LEVEL == 'debug' else False,
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
    ws.LOGGER = await ws.LOGGER.getlogger(cs.IS_LOG)
    ws.LOGGER.info({'module': name, 'info': 'Starting'})
    tasks = []
    tasks.append(ws.DBCONNECTOR_IS.connect())
    tasks.append(ws.DBCONNECTOR_WS.connect())
    tasks.append(ws.SOAPCONNECTOR.connect())
    tasks.append(ws.AMQPCONNECTOR.connect())
    await asyncio.gather(*tasks, return_exceptions=True)
    ws.LOGGER.info({'module': name, 'info': 'Started'})


@app.on_event('shutdown')
async def shutdown():
    tasks = []
    tasks.append(ws.DBCONNECTOR_IS.disconnect())
    tasks.append(ws.DBCONNECTOR_WS.disconnect())
    tasks.append(ws.SOAPCONNECTOR.disconnect())
    tasks.append(ws.AMQPCONNECTOR.disconnect())
    await asyncio.gather(*tasks, return_exceptions=True)
    await ws.LOGGER.warning({'module': name, 'info': 'Webservice is shutting down'})
    await ws.LOGGER.shutdown()


@app.get('/')
async def homepage():
    return {'title': app.title,
            'description': app.description,
            'version': app.version}


@app.get('/status')
async def rdbs():
    return {'Wisepark RDBS Connection': ws.DBCONNECTOR_WS.connected,
            'Integration RDBS Connection': ws.DBCONNECTOR_IS.connected,
            'Wisepark SOAP Connection': ws.SOAPCONNECTOR.connected}


def run():
    uvicorn.run(app=app, host=cs.IS_WEBSERVICE_HOST, port=cs.IS_WEBSERVICE_PORT, workers=cs.IS_WEBSERVICE_WORKERS, log_level=cs.IS_WEBSERVICE_LOG_LEVEL)

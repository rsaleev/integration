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
from service.routes import control, data, logs, places, services, statuses, subscription, ticket, converters
import configuration as cfg
from service import settings as ws
import nest_asyncio
from fastapi.security.api_key import APIKeyHeader, APIKey, APIKeyCookie
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import secrets
import datetime


name = 'remote'

# API_KEY = "thenb!oronaal_lazo57tathethomenasas"
# API_KEY_NAME = "token"
# TIMESTAMP = "ts"

# api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
# ts_key_header = APIKeyHeader(name=TIMESTAMP, auto_error=False)
# api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)


# async def get_api_key(
#     api_key_header: str = Security(api_key_header),
#     ts_key_header: str = Security(ts_key_header),
# ):
#     secret = hashlib.sha256((ts_key_header+API_KEY).encode()).hexdigest()
#     if api_key_header is None or api_key_header != secret or int(datetime.now().timetsmap()) - int(ts_key_header) > 5:
#         raise HTTPException(
#             status_code=HTTP_403_FORBIDDEN, detail="Unauthorized"
#         )

# security = HTTPBasic()


# def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
#     correct_username = 'admin'
#     correct_password = API_KEY
#     if not (correct_username and correct_password):
#         raise HTTPException(
#             status_code=HTTP_401_UNAUTHORIZED,
#             detail="Incorrect credentials",
#             headers={"WWW-Authenticate": "Basic"},
#         )


app = FastAPI(title="Remote management Module",
              description="Wisepark Monitoring and Remote Management Module",
              version="0.0.1", debug=cfg.asgi_debug,
              docs_url=None, redoc_url=None, openapi_url=None)

# app.include_router(control.router, dependencies=[Depends(get_api_key)])
# app.include_router(data.router, dependencies=[Depends(get_api_key)])
# app.include_router(logs.router, dependencies=[Depends(get_api_key)])
# app.include_router(places.router, dependencies=[Depends(get_api_key)])
# app.include_router(services.router, dependencies=[Depends(get_api_key)])
# app.include_router(statuses.router, dependencies=[Depends(get_api_key)])
# app.include_router(subscription.router, dependencies=[Depends(get_api_key)])
#app.include_router(ticket.router, dependencies=[Depends(get_api_key)])
#app.include_router(converters.router, dependencies=[Depends(get_api_key)])
app.include_router(converters.router)
app.include_router(ticket.router)


@app.on_event('startup')
async def startup():
    ws.logger = await ws.logger.getlogger(cfg.log)
    await ws.dbconnector_is.connect()
    await ws.dbconnector_wp.connect()
    ws.devices = await ws.dbconnector_is.callproc('is_devices_get', rows=-1, values=[None, None, None, None, None])
    wp_devices = await ws.dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
    ws.autocashiers = [d for d in wp_devices if d['terType'] == 3]
    ws.gates = [d for d in wp_devices if d['terType'] in [1, 2]]
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
async def homepage():
    return {'title': app.title,
            'description': app.description,
            'version': app.version}


@app.get('/status')
async def rdbs():
    return {'Wisepark RDBS Connection': ws.dbconnector_wp.connected,
            'Integration RDBS Connection': ws.dbconnector_is.connected,
            'Wisepark SOAP Connection': ws.soapconnector.connected}


# @app.get("/openapi.json", tags=["documentation"])
# async def get_open_api_endpoint(credentials: HTTPBasicCredentials = Depends(get_current_username)):
#     response = JSONResponse(
#         get_openapi(title="OpenAPI Documentation and Test", version="0.1", routes=app.routes)
#     )
#     return response


# @app.get("/documentation", tags=["documentation"])
# async def get_documentation(credentials: HTTPBasicCredentials = Depends(get_current_username)):
#     response = get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
#     response.set_cookie(
#         API_KEY_NAME,
#         value=api_key_header,
#         httponly=True,
#         max_age=1800,
#         expires=1800,
#     )
#     return response


def run():
    uvicorn.run(app=app, host=cfg.asgi_host, port=cfg.asgi_port, workers=cfg.asgi_workers, log_level='debug')

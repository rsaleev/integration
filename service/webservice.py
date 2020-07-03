import asyncio
import json
from datetime import datetime, timedelta
from uuid import uuid4

import toml
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from setproctitle import setproctitle
from starlette.background import BackgroundTask, BackgroundTasks
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response


from integration.service.routes import control, data, logs, places, services, devices, subscription, ticket, converters, parkconfig

import integration.service.security as iss
import configuration.settings as cs
import integration.service.settings as ws

name = 'remote'

configuration = toml.load(cs.CONFIG_FILE)

app = FastAPI(title="Integration Module",
              description="Wisepark Monitoring and Remote Management Module OpenApi schema",
              version="0.0.2 BETA",
              debug=True,
              # root_path="",
              openapi_url=None,
              docs_url=None)

app.include_router(control.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(data.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(logs.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(places.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(services.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(devices.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(subscription.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(ticket.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(converters.router, dependencies=[Depends(iss.get_api_key)])
app.include_router(parkconfig.router, dependencies=[Depends(iss.get_api_key)])


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Custom title",
        version="2.5.0",
        description="This is a very custom OpenAPI schema",
        routes=app.routes
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.on_event('startup')
async def startup():
    ws.LOGGER = await ws.LOGGER.getlogger()
    ws.LOGGER.info({'module': name, 'info': 'Starting'})
    tasks = []
    tasks.append(ws.DBCONNECTOR_IS.connect())
    tasks.append(ws.DBCONNECTOR_WS.connect())
    tasks.append(ws.SOAPCONNECTOR.connect())
    tasks.append(ws.AMQPCONNECTOR.connect())
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        print(e)
    ws.LOGGER.info({'module': name, 'info': 'Started'})


@app.on_event('shutdown')
async def shutdown():
    tasks = []
    tasks.append(ws.DBCONNECTOR_IS.disconnect())
    tasks.append(ws.DBCONNECTOR_WS.disconnect())
    tasks.append(ws.SOAPCONNECTOR.disconnect())
    tasks.append(ws.AMQPCONNECTOR.disconnect())
    await asyncio.gather(*tasks)
    await ws.LOGGER.warning({'module': name, 'info': 'Webservice is shutting down'})
    await ws.LOGGER.shutdown()
    print('closed')


@app.get("/")
async def homepage():
    return "Welcome to the security test!"


@app.get("/openapi.json")
async def get_open_api_endpoint():
    return JSONResponse(get_openapi(title="FastAPI", version=1, routes=app.routes))


@app.get("/docs")
async def get_documentation():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="docs")


def run():
    setproctitle('is-webservice')
    uvicorn.run(app=app,
                host=configuration['integration']['asgi']['host'],
                port=configuration['integration']['asgi']['port'],
                workers=configuration['integration']['asgi']['workers'],
                log_level='debug'
                )

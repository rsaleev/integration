from datetime import datetime, timedelta
from hashlib import sha256
from fastapi import Depends, Header, HTTPException, Security
from fastapi.security.api_key import APIKey, APIKeyCookie, APIKeyHeader
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.status import (HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN,
                              HTTP_407_PROXY_AUTHENTICATION_REQUIRED)

import integration.service.settings as ws

API_KEY = "thenb!oronaal_lazo57tathethomenasas"
API_KEY_NAME = "token"
TIMESTAMP = "ts"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
ts_key_header = APIKeyHeader(name=TIMESTAMP, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)


# authorization for HTTP requests

async def get_api_key(
    api_key_header: str = Security(api_key_header),
    ts_key_header: str = Security(ts_key_header),
):
    if api_key_header:
        secret = sha256(f"{ts_key_header}{API_KEY}".encode()).hexdigest()
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

# security = HTTPBasic()


# async def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
#     user = await ws.DBCONNECTOR_IS.callproc('web_user_get', credentials.username)
#     if user:
#         if sha256((f'{credentials.username}{credentials.password}').encode()) == user['userPassword']:
#             return credentials.username
#         else:
#             raise HTTPException(
#                 status_code=status.HTTP_401_UNAUTHORIZED,
#                 detail="Incorrect password",
#                 headers={"WWW-Authenticate": "Basic"},
#             )
#     else:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect login",
#             headers={"WWW-Authenticate": "Basic"},
#         )

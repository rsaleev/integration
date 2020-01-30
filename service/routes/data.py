from fastapi.routing import APIRouter
from starlette.responses import Response
from starlette.background import BackgroundTasks
import json
from pydantic import BaseModel, ValidationError, validator
import re
import dateutil.parser as dp
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
import service.settings as ws

router = APIRouter()


@router.get('/rest/monitoring/data/{view_name}')
async def get_view(view_name):
    tasks = BackgroundTasks()
    tasks.add(ws.logger)
    if re.match("ampp_", view_name):
        try:
            data = await ws.dbconnector_is.execute(f"SELECT * FROM {view_name}", rows=-1)
            return Response(json.dumps(data, default=str, ensure_ascii=False), status_code=200, media_type='application/json')
        except (OperationalError, ProgrammingError) as e:
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
            else:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Forbidden'}), status_code=403, media_type='application/json')
    else:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Forbidden'}), status_code=403, media_type='application/json')

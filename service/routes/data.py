from fastapi.routing import APIRouter
from starlette.responses import Response
from starlette.background import BackgroundTasks
import json
import re
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
import service.settings as ws

router = APIRouter()

name = 'webservice_data'


@router.get('/rest/monitoring/data/{tbl}')
async def get_view(tbl: str):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('wp_table_get', rows=-1, values=[tbl])
        return Response(json.dumps({'pageData': data}, default=str, ensure_ascii=False), status_code=200, media_type='application/json')
    except (OperationalError, ProgrammingError) as e:
        tasks.add(ws.logger.info, {'module': name, 'path': f"rest/monitoring/data/{tbl}", 'error': repr(e)})
        code, description = e.args
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Forbidden'}), status_code=403, media_type='application/json',background=tasks)

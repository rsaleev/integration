from fastapi.routing import APIRouter
from starlette.responses import Response
import json
from pydantic import BaseModel, ValidationError, validator
import re
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
from datetime import datetime
from dateutil import parser as dp
from typing import List
from aiomysql import OperationalError, DatabaseError, ProgrammingError


router = APIRouter()


class Places(BaseModel):
    type: str = 'places'
    parking_number: int
    client_free: int
    client_busy: int
    vip_client_free: int
    vip_client_busy: int
    date_event: str
    error: int = 0


@router.get('/rest/monitoring/places')
def get_places():
    data = await cfg.dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')


@router.post('/rest/monitoring/places')
def upd_places(*, places: List[Places]):
    try:
        for p in places:
            try:
                await cfg.dbconnector_wp('wp_places_upd', rows=0, values=[p.parking_number, p.client_free])
                await cfg.dbconnectow_is('is_places_upd', rows=0, values=[p.client_busy, p.vip_client_busy, p.parking_number])
    except ValidationError:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not valid request'}), status_code=403, media_type='application/json')
    except OperationalError, DatabaseError, ProgrammingError:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')

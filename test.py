from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import re
import json


class CommandRequest(BaseModel):
    type: str
    error: int 
    date_event: datetime
    device_number: Optional[int]
    device_ip: Optional[str]
    device_type: Optional[int]
    command_number: Optional[int]
    device_events_id: Optional[int]
    parking_number: Optional[int]


req = {'type': 'places', 'error': '0', 'date_event': '2012-04-05 15:23:30'}
t = CommandRequest(**req)
print(t.type)
print(t.error)
t.error = 1 
print(t.dict(exclude_unset=True))

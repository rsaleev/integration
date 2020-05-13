from pydantic import BaseModel, validator
from typing import Optional
import dateutil.parser as dp


class CommandRequest(BaseModel):
    type: str = "command"
    error: Optional[int]
    date_event: str
    came_device_id: int
    device_ip: Optional[str]
    device_type: Optional[int]
    command_number: int
    device_events_id: Optional[int]

    @validator('date_event')
    def date_validator(cls, v):
        dt = dp.parse(v)
        return dt


test = {'date_event': '17-05-2020 17:05:30', 'came_device_id': '1', 'command_number': '1'}
command = CommandRequest(**test)
print(command.date_event)

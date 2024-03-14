from typing import Optional
from pydantic import BaseModel

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: str | None
    last_seen: str
    type: str
    mac_address: str | None
    battery_level: float
    temperature: float | None
    humidity: float | None
    velocity: float | None
    
    
    
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str

class SensorData(BaseModel):
    velocity: float | None
    temperature: float | None
    humidity: float | None
    battery_level: float
    last_seen: str
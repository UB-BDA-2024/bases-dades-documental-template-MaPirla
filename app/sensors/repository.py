from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas
from app import sensors

def get_sensor(db: Session, sensor_id: int) -> models.Sensor | None:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    for e in data:
        if e[1] != None:
            redis.set(str(sensor_id)+"_"+e[0], e[1])
    return data

def get_data(redis: Session, db: Session, sensor_id: int) -> schemas.Sensor:
    
    model = get_sensor(db, sensor_id)
    if model == None:
        raise HTTPException(status_code=404, detail="Sensor not found")


    sensor = schemas.Sensor(
        id=model.id,
        name=model.name,
        latitude=model.latitude,
        longitude=model.longitude,
        joined_at=str(model.joined_at),
        last_seen = redis.get(str(sensor_id)+"_last_seen"),
        type=model.type,
        mac_address= model.mac_address,
        battery_level = redis.get(str(sensor_id)+"_battery_level"),
        temperature= redis.get(str(sensor_id)+"_temperature"),
        humidity=redis.get(str(sensor_id)+"_humidity"),
        velocity=redis.get(str(sensor_id)+"_velocity")
    )
    return sensor

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient

from . import models, schemas
from app import sensors

def get_sensor(db: Session, sensor_id: int) -> models.Sensor | None:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session,mongodb: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    sensor_json = {
        "id_sensor": db_sensor.id,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        }
    }
    mongodb.getDatabase("MongoDB_")
    collection = mongodb.getCollection("sensor")
    collection.insert_one(sensor_json)
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

def delete_sensor(db: Session,redis:RedisClient, mongodb:MongoDBClient, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    mongodb.getDatabase('MongoDB_')
    mongodb.getCollection('sensor')
    mongodb.collection.delete_one({"id_sensor": sensor_id})
    # delete from posgreSQL
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, db: Session, redis:RedisClient, latitude: float, longitude: float, radius: int):
    mongodb.getDatabase("MongoDB_")
    collection = mongodb.getCollection("sensor")
    collection.create_index([("location", "2dsphere")])

    query = {
        "location": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "$maxDistance": radius  
            }
        }
    }

    near_sensors = list(collection.find(query))
    print(near_sensors)
    sensors = []
    for sensor in near_sensors:
        sensors.append(get_data(db=db,redis=redis, sensor_id=sensor["id_sensor"]))
    print(sensors)
    return sensors

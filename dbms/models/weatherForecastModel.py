from dbms import db
from datetime import datetime


class WeatherForecast(db.Model):
    __tablename__ = 'weather_forecast'

    id = db.Column(db.Integer, primary_key=True, index=True)
    latitude = db.Column(db.String)
    longitude = db.Column(db.String)
    updated_date_utc = db.Column(db.DateTime)
    forecast_date_utc = db.Column(db.DateTime)
    air_temperature = db.Column(db.Numeric(8, 3))
    soil_temperature = db.Column(db.Numeric(8, 3))
    volumetric_soil_moisture_content = db.Column(db.Numeric(8, 3))
    rainfall_boolean = db.Column(db.Boolean, default=False)
    precipitation_rate = db.Column(db.Numeric(8, 3))
    relative_humidity = db.Column(db.Numeric(8, 3))
    dew_point_temperature = db.Column(db.Numeric(8, 3))
    pressure_reduced_to_msl = db.Column(db.Numeric(8, 3))
    pressure = db.Column(db.Numeric(8, 3))
    wind_speed = db.Column(db.Numeric(8, 3))
    total_cloud_cover = db.Column(db.Numeric(8, 3))
    forecast_date_local = db.Column(db.DateTime)

    # def __init__(self, latitude, longitude, updated_date_utc, forecast_date_utc, air_temperature, soil_temperature,
    #              volumetric_soil_moisture_content, rainfall_boolean, precipitation_rate, relative_humidity,
    #              dew_point_temperature, pressure_reduced_to_msl, pressure, wind_speed, total_cloud_cover,
    #              forecast_date_local):
    def __init__(self, latitude, longitude, updated_date_utc, forecast_date_utc, forecast_date_local, record):
        self.latitude = latitude,
        self.longitude = longitude,
        self.updated_date_utc = updated_date_utc,
        self.forecast_date_utc = forecast_date_utc,
        self.air_temperature = record[0],
        self.soil_temperature = record[1],
        self.volumetric_soil_moisture_content = record[2],
        self.rainfall_boolean = record[3],
        self.precipitation_rate = record[4],
        self.relative_humidity = record[5],
        self.dew_point_temperature = record[6],
        self.pressure_reduced_to_msl = record[7],
        self.pressure = record[8],
        self.wind_speed = record[9],
        self.total_cloud_cover = record[10],
        self.forecast_date_local = forecast_date_local

    def __repr__(self):
        return '<id {}>'.format(self.id)
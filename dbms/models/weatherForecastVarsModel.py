from dbms import db
from datetime import datetime


class WeatherForecastVars(db.Model):
    __tablename__ = 'weather_forecast_vars'

    id = db.Column(db.Integer, primary_key=True, index=True)
    tm_arr = db.Column(db.String(1000), nullable=True)

    def __init__(self, tm_arr):
        self.tm_arr = tm_arr

    def __repr__(self):
        return '<id {}>'.format(self.id)

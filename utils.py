import timezonefinder
import pytz
from datetime import datetime

from dbms.models.weatherForecastModel import WeatherForecast
from dbms import app, db
from dbms.models.weatherForecastVarsModel import WeatherForecastVars


class Utils:
    @staticmethod
    def insert_var_val_4d_db(lats, lons, var_val4D, tm_arr, updatedDt):
        try:
            with app.app_context():
                WeatherForecast.query.delete()  # truncate the table
            shape = var_val4D.shape
            for lats_idx in range(shape[0]):  # lats
                # for lats_idx in range(2):  # lats
                print(f":::   {lats_idx + 1}/720   ::: batch of lats")
                records = []
                for lons_idx in range(shape[1]):  # lons
                    # for lons_idx in range(2):  # lons
                    for rows_idx in range(shape[2]):  # rows
                        features_data = var_val4D[lats_idx][lons_idx][rows_idx].tolist()
                    weather_record = WeatherForecast(lats[lats_idx], lons[lons_idx], updatedDt,
                                                     datetime.fromtimestamp(tm_arr[rows_idx]),
                                                     Utils.fetchLocalTime(lats[lats_idx], lons[lons_idx],
                                                                          tm_arr[rows_idx]),
                                                     features_data)
                    weather_record.rainfall_boolean = features_data[7]
                    records.append(weather_record)
                with app.app_context():
                    db.session.bulk_save_objects(records, return_defaults=True)
                    db.session.commit()
                    print(f"Records saved successfully for batch -----> {lats_idx + 1}/720 of lats")

        except Exception as e:
            raise e

    @staticmethod
    def fetchLocalTime(lat, lon, forecast_date_utc):
        forecast_dts = datetime.utcfromtimestamp(int(forecast_date_utc))
        tf = timezonefinder.TimezoneFinder()
        # From the lat/long, get the tz-database-style time zone name (e.g. 'America/Vancouver') or None
        timezone_str = tf.certain_timezone_at(lat=float(lat), lng=float(lon))
        timezone = pytz.timezone(timezone_str)
        return forecast_dts + timezone.utcoffset(forecast_dts)

    @staticmethod
    def insert_vars_data_db(tm_arr):
        # Need to populate the WeatherForecastVars table
        with app.app_context():
            WeatherForecastVars.query.delete()  # truncate the table

            weather_vars_records = [WeatherForecastVars(record) for record in tm_arr]
            db.session.bulk_save_objects(weather_vars_records, return_defaults=True)
            db.session.commit()

    @staticmethod
    def fetch_vars_data_db():
        with app.app_context():
            try:
                result = WeatherForecastVars.query.all()
                data = [r.tm_arr for r in result]
                return data
            except Exception as e:
                raise e

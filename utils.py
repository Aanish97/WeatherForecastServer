import timezonefinder
import pytz
from datetime import datetime

from dbms.models.weatherForecastModel import WeatherForecast
from dbms import app, db


class Utils:
    @staticmethod
    def insert_var_val_4d_db(lats, lons, var_val4D, tm_arr, updatedDt):
        try:
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

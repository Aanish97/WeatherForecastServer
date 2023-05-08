from dbms.models.weatherForecastModel import WeatherForecast


class Utils:
    @staticmethod
    def insert_var_val_4d_db(var_val4D, updatedDtStr):
        records = []
        shape = var_val4D.shape
        for lats_idx in range(shape[0]):  # lats
            for lons_idx in range(shape[1]):  # lons
                for rows_idx in range(shape[2]):  # rows
                    WeatherForecast(var_val4D[lats_idx], updatedDtStr, updatedDtStr, updatedDtStr, var_val4D[lats_idx][lons_idx][rows_idx])
        return

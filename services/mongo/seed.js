// Leer los datos del fichero de datos
var text = cat('/data/data.csv')

db.model_data.insert({"data_name" : "data_model_1", "content" : text, "arima_hum_model_filename": "arima_model_hum.pkl", "arima_temp_model_filename": "arima_model_temp.pkl", "prophet_hum_model_filename": "prophet_model_hum.pkl", "prophet_temp_model_filename": "prophet_model_temp.pkl"  });
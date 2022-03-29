# data source
immigration_data_path = "sas_data"
temperature_data_path = "GlobalLandTemperaturesByCity.csv"
graphic_data_path = "us-cities-demographics.csv"

# Data for fact table and dimension tables
fact_immigration_path = "result/fact_immigration_data"
dim_immi_personal_path = "result/dim_immi_personal_data"
dim_immi_airline_path = "result/dim_immi_airline_data"
dim_temperature_path = "result/dim_temperature_data"
dim_graphic_population_path = "result/dim_graphic_population_data"
dim_graphic_statistics_path = "result/dim_graphic_statistics_data"

list_target_paths = [fact_immigration_path, 
              dim_immi_personal_path, 
              dim_immi_airline_path, 
              dim_temperature_path, 
              dim_graphic_population_path, 
              dim_graphic_statistics_path]
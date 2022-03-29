## Project structure

`result`: Folder contains all data of tables after ETL processing </br>
`check_quality.py`: Script to check data quality after ETL processing </br>
`etl.py`: Script to run all ETL processes </br>
`variables.py`: Contains variables for file paths </br>
`sas_data`: Contains raw immigration data </br>
`immigration_data_sample.csv`: Sample data after convert SAS format to csv by Spark </br>
`us-cities-demographics.csv`: Data for cities demographics of US </br>
`GlobalLandTemperaturesByCity.csv`: Data for temperature by city </br>
`Capstone Project Template.ipynb`: Contains all requirements of this capstone project </br>

## How to run this project
1. Confirm that there are all data files (sas_data, us-cities-demographics.csv, GlobalLandTemperaturesByCity.csv)
2. Run the ETL process: `python etl.py`
3. Check data quality: `python check_quality.py`



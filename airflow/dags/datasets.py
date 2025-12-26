from airflow.datasets import Dataset

weather_ready = Dataset("dataset://weather_loaded")
trip_ready = Dataset("dataset://trip_loaded")
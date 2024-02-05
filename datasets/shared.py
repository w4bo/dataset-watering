import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import dotenv_values
import numpy as np
config = dotenv_values(".env")

out_db_params = {
    'host': config["OUT_HOST"],
    'database': config["OUT_DB"],
    'user': config["OUT_USER"],
    'password': config["OUT_PWD"],
    'port': config["OUT_PORT"]
}

standard_tables = {
    "dt_field": {"col": ["field", "province", "region", "country"]},
    "dt_time": {"col": ["timestamp", "datetime", "hour", "date", "month", "year", "week", "week_in_year"]},
    "dt_agent": {"col": ["agent", "agentType", "agentHier"]},
    "ft_measurement": {"col": ["agent", "type", "field", "owner", "project", "timestamp", "value", "delay"]},
}

def get_connection(db_params):
    # Connect to PostgreSQL server
    conn = psycopg2.connect(
        dbname=db_params["OUT_DB"],
        user=db_params["OUT_USER"],
        password=db_params["OUT_PWD"],
        host=db_params["OUT_HOST"],
        port=db_params["OUT_PORT"]
    )
    conn.autocommit = True
    return conn

def get_tables():
    return dict(standard_tables)

def get_columns(tables):
    return list(set([item for sublist in [c["col"] + (c["json"] if "json" in c else []) for c in tables.values()] for item in sublist]))

def row_to_json(row):
    row = {key: value for key, value in row.to_dict().items() if not pd.isna(value) and value is not None}
    return json.dumps(row)

def compute_json(df, df_all, cols):
    if isinstance(cols, dict):
        jso = cols["json"] if "json" in cols else []
    else:
        jso = []
    if jso == []:
        df["rawJSON"] = json.dumps({})
    else:
        df["rawJSON"] = df_all[list(set(jso) & set(list(df_all.columns)))].apply(lambda row: row_to_json(row), axis=1)
    return df

def extend_df(sdf, owner, project):
    sdf["datetime"] = pd.to_datetime(sdf["timestamp"], unit="s")
    sdf["date"] = sdf["datetime"].dt.date
    sdf["month"] = sdf["datetime"].dt.month
    sdf["year"] = sdf["datetime"].dt.year
    sdf["month"] = sdf.apply(lambda x: "{}-{}".format(x["year"], x["month"]), axis=1)
    sdf["hour"] = sdf["datetime"].dt.hour
    sdf["hour"] = sdf.apply(lambda x: "{} {}:00:00".format(x["date"], x["hour"]), axis=1)
    sdf['week'] = sdf["datetime"].dt.isocalendar().week
    sdf['week_in_year'] = sdf.apply(lambda x: '{}-{}'.format(x["year"], x["week"]), axis=1)
    sdf["timestampReceived"] = sdf["timestamp"]
    sdf["delay"] = sdf["timestampReceived"] - sdf["timestamp"] 
    sdf["province"] = "FE"
    sdf["region"] = "ER"
    sdf["country"] = "IT"
    sdf["owner"] = owner
    sdf["project"] = project
    sdf["agentType"] = sdf.apply(lambda x: get_agent_type(x["type"], x), axis=1)
    sdf["agent"] = sdf.apply(lambda x: x["agentType"] + "-" + x["field"].split('-')[1], axis=1)
    sdf["agentHier"] = sdf.apply(lambda x: get_agent_hier(x["type"], x), axis=1)
    sdf["type-ext"] = sdf.apply(lambda x: get_meas_type(x["type"], x), axis=1)

def get_agent_type(s, x):
    if s == "DRIPPER":
        return "Dripper"
    elif s in ["GROUND_WATER_POTENTIAL", "GRND_WATER_G", "GROUND_SATURATION_DEGREE"]:
        return "Sensor-({},{},{})".format(x["x"], x["y"], x["z"]).replace("nan", "0").replace(".0", "")
    elif s in ["SOLAR_RAD", "AIR_HUM", "WIND_DIRECTION", "AIR_TEMP", "WIND_GUST_MAX", "PLUV_CURR", "WIND_SPEED"]:
        return "WeatherStation"
    else:
        return s
    
def get_agent_hier(s, x):
    if s in ["ETC", "BIG", "ET0"]:
        return "Process"
    else:
        return "AssignedDevice"

def get_meas_type(s, x):
    if s in ["GROUND_WATER_POTENTIAL", "GRND_WATER_G", "GROUND_SATURATION_DEGREE"]:
        return "{}-({},{},{})".format(x["type"], x["x"], x["y"], x["z"]).replace("nan", "0").replace("nan", "0").replace(".0", "")
    else:
        return s

def get_engine(db_params, type="postgresql"):
    conn_str = "://{user}:{password}@{host}:{port}/{database}".format(**db_params)
    if type == "postgresql":
        conn_str = "postgresql+psycopg2" + conn_str
    else:
        conn_str = "oracle+cx_oracle" + conn_str
    print(conn_str)
    return create_engine(conn_str)

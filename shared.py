import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import dotenv_values
config = dotenv_values(".env")

def get_columns(tables):
    return list(set([item for sublist in [c["col"] + (c["json"] if "json" in c else []) for c in tables.values()] for item in sublist]))

def row_to_json(row):
    row = {key: value for key, value in row.to_dict().items() if not pd.isna(value)}
    return json.dumps(row)

def compute_json(df, df_all, cols):
    if isinstance(cols, dict):
        jso = cols["json"] if "json" in cols else []
    else:
        jso = []
    if jso == []:
        df["rawJSON"] = json.dumps({})
    else:
        df["rawJSON"] = df_all[jso].apply(lambda row: row_to_json(row), axis=1)
    return df

def extend_df(sdf, owner, project):
    sdf["datetime"] = pd.to_datetime(sdf["timestamp"], unit="s")
    sdf["date"] = pd.to_datetime(sdf["timestamp"], unit="s").dt.date
    sdf["month"] = pd.to_datetime(sdf["timestamp"], unit="s").dt.month
    sdf["year"] = pd.to_datetime(sdf["timestamp"], unit="s").dt.year
    sdf["month"] = sdf.apply(lambda x: "{}-{}".format(x["year"], x["month"]), axis=1)
    sdf["hour"] = pd.to_datetime(sdf["timestamp"], unit="s").dt.hour
    sdf["hour"] = sdf.apply(lambda x: "{} {}:00:00".format(x["date"], x["hour"]), axis=1)
    sdf["timestampReceived"] = sdf["timestamp"]
    sdf["delay"] = sdf["timestampReceived"] - sdf["timestamp"] 
    sdf["province"] = "FE"
    sdf["region"] = "ER"
    sdf["country"] = "IT"
    sdf["owner"] = owner
    sdf["project"] = project
    sdf["agent"] = sdf.apply(lambda x: get_agent(x["type"], x), axis=1)
    sdf["agentType"] = sdf.apply(lambda x: get_agenttype(x["type"], x), axis=1)
    sdf["type-ext"] = sdf.apply(lambda x: get_meastype(x["type"], x), axis=1)

def get_agent(s, x):
    if s == "DRIPPER":
        return "Dripper"
    elif s in ["GROUND_WATER_POTENTIAL", "GRND_WATER_G", "GROUND_SATURATION_DEGREE"]:
        return "sensor-({},{},{})".format(x["x"], x["y"], x["z"]).replace("nan", "0").replace(".0", "")
    elif s in ["SOLAR_RAD", "AIR_HUM", "WIND_DIRECTION", "AIR_TEMP", "WIND_GUST_MAX", "PLUV_CURR", "WIND_SPEED"]:
        return "WeatherStation"
    else:
        return s
    
def get_agenttype(s, x):
    if s in ["ETC", "BIG", "ET0"]:
        return "Process"
    else:
        return "AssignedDevice"

def get_meastype(s, x):
    if s in ["GROUND_WATER_POTENTIAL", "GRND_WATER_G", "GROUND_SATURATION_DEGREE"]:
        return "{}-({},{},{})".format(x["type"], x["x"], x["y"], x["z"]).replace("nan", "0").replace("nan", "0").replace(".0", "")
    else:
        return s
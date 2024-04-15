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
    "dt_time": {"col": ["timestamp", "datetime", "hour", "hour_in_day", "date", "month", "month_in_year", "year", "week", "week_in_year"]},
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

def ext_date(dt):
    t = 'timestamp'
    dt['timestamp'] = pd.to_datetime(dt[t], unit='s')
    dt['datetime'] = dt[t]
    dt['date'] = dt[t].dt.strftime('%Y-%m-%d') # .dt.date 
    dt['year'] = dt[t].dt.year
    dt['month_in_year'] = dt[t].dt.month
    dt['month'] = dt[t].dt.strftime('%Y-%m')
    dt['week_in_year'] = dt[t].dt.isocalendar().week
    dt['week'] = dt.apply(lambda x: '{}-{}'.format(x["year"], x["week_in_year"]), axis=1)
    dt["hour_in_day"] = dt[t].dt.hour.astype("int32")
    dt["hour"] = dt[t].dt.strftime('%Y-%m-%d %H:00:00')

def extend_df(sdf, owner, project):
    ext_date(sdf)
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

def clean_and_create_metadata(engine, drop_only=False):
    statements = [
        """DROP TABLE database CASCADE CONSTRAINTS""",
        """CREATE TABLE database (
        database_id varchar2(255) NOT NULL,
        database_name varchar2(255) NOT NULL,
        IPaddress varchar2(16) NOT NULL,
        port NUMBER NOT NULL,
        PRIMARY KEY (database_id),
        UNIQUE(database_name, IPaddress, port)
        )""",
        """DROP TABLE groupbyoperator CASCADE CONSTRAINTS""",
        """CREATE TABLE groupbyoperator (
        groupbyoperator_id varchar2(255) NOT NULL,
        groupbyoperator_name varchar2(255) NOT NULL UNIQUE,
        groupbyoperator_synonyms varchar2(1000),
        PRIMARY KEY (groupbyoperator_id)
        )""",
        """DROP TABLE hierarchy CASCADE CONSTRAINTS""",
        """CREATE TABLE hierarchy (
        hierarchy_id varchar2(255) NOT NULL,
        hierarchy_name varchar2(255) NOT NULL UNIQUE,
        hierarchy_synonyms varchar2(1000),
        PRIMARY KEY (hierarchy_id)
        )""",
        """DROP TABLE fact CASCADE CONSTRAINTS""",
        """CREATE TABLE fact (
        fact_id varchar2(255) NOT NULL,
        fact_name varchar2(255) NOT NULL UNIQUE,
        fact_synonyms varchar2(1000),
        database_id varchar2(255) NULL REFERENCES database (database_id) ON DELETE CASCADE,
        PRIMARY KEY (fact_id)
        )""",
        """DROP TABLE "TABLE" CASCADE CONSTRAINTS""",
        """CREATE TABLE "TABLE" (
        table_id varchar2(255) NOT NULL,
        table_name varchar2(255) NOT NULL UNIQUE,
        table_type varchar2(255) NOT NULL,
        fact_id varchar2(255) DEFAULT NULL REFERENCES fact (fact_id),
        hierarchy_id varchar2(255) DEFAULT NULL REFERENCES hierarchy (hierarchy_id) ON DELETE CASCADE,
        PRIMARY KEY (table_id)
        )""",
        """DROP TABLE relationship CASCADE CONSTRAINTS""",
        """CREATE TABLE relationship (
        relationship_id varchar2(255) NOT NULL,
        table1 varchar2(255) NOT NULL REFERENCES "TABLE" (table_id) ON DELETE CASCADE,
        table2 varchar2(255) NOT NULL REFERENCES "TABLE" (table_id) ON DELETE CASCADE,
        PRIMARY KEY (relationship_id)
        )""",
        """DROP TABLE "COLUMN" CASCADE CONSTRAINTS""",
        """CREATE TABLE "COLUMN" (
        column_id varchar2(255) NOT NULL,
        column_name varchar2(255) NOT NULL,
        column_type varchar2(255) NOT NULL,
        isKey number(1)  NOT NULL,
        relationship_id varchar2(255) DEFAULT NULL,
        table_id varchar2(255) NOT NULL REFERENCES "TABLE"(table_id) ON DELETE CASCADE,
        PRIMARY KEY (column_id) -- , UNIQUE (column_name, table_id)
        )""",
        """DROP TABLE "LEVEL" CASCADE CONSTRAINTS""",
        """CREATE TABLE "LEVEL" (
        level_id varchar2(255) NOT NULL,
        level_type varchar2(255) NOT NULL,
        level_description varchar2(200),
        level_name varchar2(255) NOT NULL, -- UNIQUE,
        cardinality NUMBER DEFAULT NULL,
        hierarchy_id varchar2(255) NOT NULL REFERENCES "HIERARCHY" (hierarchy_id) ON DELETE CASCADE,
        level_synonyms varchar2(1000),
        column_id varchar2(255) NOT NULL REFERENCES "COLUMN"(column_id),
        "MIN" DOUBLE PRECISION DEFAULT NULL,
        "MAX" DOUBLE PRECISION DEFAULT NULL,
        "AVG" DOUBLE PRECISION DEFAULT NULL,
        isDescriptive NUMBER(1) DEFAULT 0,
        mindate DATE DEFAULT NULL,
        maxdate DATE DEFAULT NULL,
        PRIMARY KEY (level_id)
        )""",
        """DROP TABLE hierarchy_in_fact CASCADE CONSTRAINTS""",
        """CREATE TABLE hierarchy_in_fact (
        fact_id varchar2(255) NOT NULL REFERENCES fact (fact_id),
        hierarchy_id varchar2(255) NOT NULL REFERENCES hierarchy (hierarchy_id) ON DELETE CASCADE,
        PRIMARY KEY (fact_id, hierarchy_id)
        )""",
        """DROP TABLE language_predicate CASCADE CONSTRAINTS""",
        """CREATE TABLE language_predicate (
        language_predicate_id varchar2(255) NOT NULL,
        language_predicate_name varchar2(255) NOT NULL UNIQUE,
        language_predicate_synonyms varchar2(1000) DEFAULT NULL,
        language_predicate_type varchar2(255) DEFAULT NULL,
        PRIMARY KEY (language_predicate_id)
        )""",
        """DROP TABLE language_operator CASCADE CONSTRAINTS""",
        """CREATE TABLE language_operator (
        language_operator_id varchar2(255) NOT NULL,
        language_operator_name varchar2(255) NOT NULL UNIQUE,
        language_operator_synonyms varchar2(1000) DEFAULT NULL,
        language_operator_type varchar2(255) DEFAULT NULL,
        PRIMARY KEY (language_operator_id)
        )""",
        """DROP TABLE measure CASCADE CONSTRAINTS""",
        """CREATE TABLE measure (
        measure_id varchar2(255) NOT NULL,
        measure_name varchar2(255) NOT NULL,
        fact_id varchar2(255) NOT NULL REFERENCES fact (fact_id),
        measure_synonyms varchar2(1000),
        column_id varchar2(255) NOT NULL REFERENCES "COLUMN" (column_id) ON DELETE CASCADE,
        PRIMARY KEY (measure_id),
        UNIQUE(measure_name, fact_id)
        )""",
        """DROP TABLE member CASCADE CONSTRAINTS""",
        """CREATE TABLE member (
        member_id varchar2(255) NOT NULL,
        member_name varchar2(255) NOT NULL,
        level_id varchar2(255) NOT NULL REFERENCES "LEVEL" (level_id) ON DELETE CASCADE,
        member_synonyms varchar2(1000),
        PRIMARY KEY (member_id),
        UNIQUE(member_name, level_id)
        )""",
        """DROP TABLE groupbyoperator_of_measure CASCADE CONSTRAINTS""",
        """CREATE TABLE groupbyoperator_of_measure (
        groupbyoperator_id varchar2(255) NOT NULL REFERENCES groupbyoperator (groupbyoperator_id) ON DELETE CASCADE,
        measure_id varchar2(255) NOT NULL REFERENCES measure (measure_id) ON DELETE CASCADE,
        PRIMARY KEY (groupbyoperator_id, measure_id)
        )""",
        """DROP TABLE "SYNONYM" CASCADE CONSTRAINTS""",
        """CREATE TABLE "SYNONYM" (
        synonym_id varchar2(255) NOT NULL,
        table_name varchar2(255) NOT NULL,
        reference_id varchar2(255) NOT NULL,
        "TERM" varchar2(255) NOT NULL,
        PRIMARY KEY (synonym_id),
        UNIQUE(term, reference_id, table_name)
        )""",
        """DROP TABLE OLAPSESSION CASCADE CONSTRAINTS""",
        """CREATE TABLE OLAPSESSION (
        "TIMESTAMP" NUMBER,
        session_id varchar2(255),
        annotation_id varchar2(255),
        value_en varchar2(1000),
        value_ita varchar2(1000),
        limit long,
        fullquery_serialized blob,
        fullquery_tree varchar2(1000),
        olapoperator_serialized blob
        )"""
    ]
    for statement in statements:
        try:
            if drop_only and "DROP" not in statement: continue
            engine.execute(statement)
        except Exception as e:
            print(statement)
            print(e)
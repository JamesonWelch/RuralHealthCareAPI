import requests
import json
import pandas as pd
import io
import os
import sys

# ROOT_DIR = os.path.join(sys._MEIPASS, "../../..")
# DATA_DIR = os.path.join(ROOT_DIR, "Data")
# LOG_DIR = os.path.join(DATA_DIR, "Logs")


urlJson = "https://opendata.usac.org/resource/avi8-svp9.json?$select=count(*)"
urlCsv = "https://opendata.usac.org/resource/avi8-svp9.csv"

df_json = requests.get(urlJson)
df_csv = requests.get(urlCsv).content


def dataFrame(url, content):
    r = requests.get(url)
    if r.ok:
        data = r.content.decode("utf8")
        df = pd.read_csv(io.StringIO(data))
    if content == "csv":
        df.to_csv(os.path.join('large_ds_', "test.csv"))
    elif content == "json":
        pass


dataFrame(urlCsv, content="csv")

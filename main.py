import sqlite3
import pandas as pd

from typing import List
from unicodedata import normalize, combining

from settings import FILENAME


def remove_accent_and_upper(texto: str) -> str:
    normalized: str = normalize("NFD", texto)
    str_to_filter: str = "".join(c for c in normalized if not combining(c))

    return normalize("NFC", str_to_filter).upper().strip()

df: pd.DataFrame = pd.read_excel(f"./{FILENAME}")

columns_name: List[str] = [
    "municipio",
    "cod_municipio",
    "cod_reg_imediata",
    "regiao_imediata",
    "cod_reg_intermediaria",
    "regiao_intermediaria",
]
str_columns: List[str] = ["municipio", "regiao_imediata", "regiao_intermediaria"]

df.columns = columns_name

for column_name in list(df.columns):
    if column_name in str_columns:
        df[column_name] = df[column_name].apply(remove_accent_and_upper)


micro = df.groupby("regiao_imediata", as_index=False)["municipio"].apply(lambda x: ",".join(x))
micro.columns = ["regiao_imediata", "municipios"]
meso = df.groupby("regiao_intermediaria", as_index=False)["municipio"].apply(lambda x: ",".join(x))
meso.columns = ["regiao_intermediaria", "municipios"]

with sqlite3.connect("./mesodb.db") as cnx:
    cur = cnx.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS mesoregioes (municipio TEXT, cod_municipio NUMBER, cod_reg_imediata NUMBER, regiao_imediata TEXT, cod_regiao_intermediaria NUMBER, regiao_intermediaria TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS regiao_imediata (regiao_imediata TEXT, municipios text)")
    cur.execute("CREATE TABLE IF NOT EXISTS regiao_intermediaria (regiao_intermediaria TEXT, municipios text)")
    df.to_sql(name="mesoregioes", con=cnx, if_exists="replace")
    micro.to_sql(name="regiao_imediata", con=cnx, if_exists="replace")
    cur.execute("CREATE INDEX IF NOT EXISTS regiao_imediata_agg ON regiao_imediata (municipios)")
    meso.to_sql(name="regiao_intermediaria", con=cnx, if_exists="replace")
    cur.execute("CREATE INDEX IF NOT EXISTS regiao_intermediaria_agg ON regiao_intermediaria (municipios)")


with sqlite3.connect("./mesodb.db") as cnx:
    cur = cnx.cursor()
    for value in cur.execute("SELECT municipios FROM regiao_intermediaria WHERE municipios LIKE '%GUAXUPE%'"):
        print(value[0], len(value[0].split(",")), type(value[0]))


# cur.execute("DROP TABLE mesoregioes;")


df.to_csv(f"""./{FILENAME.replace(".xls", ".csv")}""", index=False)



cur.execute("SELECT * FROM mesoregioes GROUP BY regiao_imediata")
import sqlite3
import pandas as pd

from typing import List
from unicodedata import normalize, combining

from settings import FILENAME


def remove_accent_and_upper(texto: str) -> str:
    normalized: str = normalize("NFD", texto)
    str_to_filter: str = "".join(c for c in normalized if not combining(c))

    return normalize("NFC", str_to_filter).upper().strip()

cnx = sqlite3.connect("./mesodb.db")

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

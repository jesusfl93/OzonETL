# python modules
import datetime
import certifi
import pymongo
import pandas as pd
import numpy as np
from bson import ObjectId
from pymongo import MongoClient

ca = certifi.where()
CLIENT = pymongo.MongoClient("mongodb+srv://jesuscastellanosfl:90lbxXWQHSd8HroW@ozontechinterview.oddbjn5.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=ca)
DB = CLIENT["Ozon_test"]
COLLECTION = DB["vehicles_data"]

# extraer data
print("Cargando archivos de origen")
vehicleslData = pd.read_excel("BDMotos.xlsx", sheet_name="vehiculos", header=0)
brandsData = pd.read_csv("brands.csv", delimiter=",")
df = vehicleslData[
    [
        "cuota semanal descuento",
        "año ",
        "kilometraje_aprox",
        "cuota",
        "id_ozon",
        "serie_vehicular_o_num_chasis",
        "num_motor",
        "gasto_compra",
        "Color",
        "cilindraje",
        "marca",
        "pais",
        "placa",
        "num_tarjeta_circ",
    ]
]
#transformar data
# renombrar columnas para hacer match con el ETL
print("Iniciando transformaciones")
etl_df = df.copy()
etl_df.rename(
    columns={
        "cuota semanal descuento": "salePrice",
        "año ": "details.year",
        "kilometraje_aprox": "details.milage",
        "cuota": "oldPrice",
        "id_ozon": "internalId",
        "serie_vehicular_o_num_chasis": "vehicleSN",
        "num_motor": "engineSN",
        "gasto_compra": "purchaseCost",
        "Color": "color",
        "cilindraje": "cylindersCapacity",
        "marca": "brand",
        "pais": "country",
        "placa": "plate",
        "num_tarjeta_circ": "registrationCard",
    },
    inplace=True,
)

# Agregar Columna temporal para realizar las validaciones de la cuota y precio viejo
etl_df = etl_df.assign(tempSalesPrice=etl_df["salePrice"])

# Limpiar los valores NaN
etl_df = etl_df[etl_df["internalId"].notna()]

# Remover motos de otros paises y dejar unicamente OMX
etl_df["idCountry"] = etl_df["internalId"].str.slice(0, 3)
etl_df.drop(etl_df.loc[etl_df["idCountry"] != "OMX"].index, inplace=True)
etl_df = etl_df.drop(columns=["idCountry"])

# Eliminar los registros que sean mayores a la moto 1000
rows_limit = etl_df[etl_df["internalId"] == "OMX1000"].index.tolist()
etl_df = etl_df.iloc[: rows_limit[0] - 1]

# En caso de existir “precio descuento”, insertar la columna “cuota semanal descuento" si no "cuota"
etl_df["salePrice"] = etl_df["salePrice"].fillna(etl_df["oldPrice"])

# En caso de existir “precio descuento”, se deberá insertar la columna ”cuota” sino 0.
etl_df.loc[etl_df["tempSalesPrice"].notnull() == False, "oldPrice"] = 0
etl_df = etl_df.drop(columns=["tempSalesPrice"])

# En caso de una combinación tomar sólo el primero
# Validaciones adicionales: dejar la columna en mayuscula
etl_df["color"] = etl_df["color"].str.split("/").str[0]
etl_df["color"] = etl_df["color"].str.upper()

# Validar la columna brand en mayuscula
etl_df["brand"] = etl_df["brand"].str.upper()

# Buscar el Id de la marca en el archivo brands
etl_df.astype({"brand": "string"}).dtypes
brandsData.astype({"name": "string"}).dtypes

etl_df = pd.merge(
    etl_df, brandsData[["name", "_id"]], left_on="brand", right_on="name", how="left"
).drop(columns=["name"])

etl_df = etl_df.drop(["brand"], axis=1)
etl_df = etl_df.rename(columns={"_id": "brand"})
etl_df = etl_df.replace({np.nan: None})

# Convertir a dataframe a Json
etl_records = etl_df.to_dict("records")
arrData = []

for index in range(len(etl_records)):
    record = {
        "_id": ObjectId(),
        "salePrice": etl_records[index]["salePrice"],
        "details": {
            "year": etl_records[index]["details.year"],
            "milage": etl_records[index]["details.milage"],
        },
        "oldPrice": etl_records[index]["oldPrice"],
        "internalId": etl_records[index]["internalId"],
        "vehicleSN": etl_records[index]["vehicleSN"],
        "engineSN": etl_records[index]["engineSN"],
        "purchaseCost": etl_records[index]["purchaseCost"],
        "color": etl_records[index]["color"],
        "cylindersCapacity": etl_records[index]["cylindersCapacity"],
        "brand": ObjectId(etl_records[1]["brand"]),
        "createdAt": datetime.datetime.now().isoformat(),
        "updatedAt": datetime.datetime.now().isoformat(),
        "country": etl_records[index]["country"],
        "plate": etl_records[index]["plate"],
        "registrationCard": etl_records[index]["registrationCard"],
    }
    arrData.append(record)

#Cargar - Insertar Data
COLLECTION.insert_many(arrData)
print("Datos cargados satisfactoriamente")

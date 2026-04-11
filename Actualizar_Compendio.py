import io
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SPREADSHEET_ID = "1h0ufsJz8d94uFKs83hdyJQUR57mmFtC8wvFHwNDTFFE"
FOLDER_ID = "1cf2p3R7iM0xowAt4muEruDwxZoZqD_jB"

HOJA_DATA = "Data"
HOJA_DESTINO = "Cartera mes anterior"

COLUMNAS_NECESARIAS = [
    "Referencia",
    "Id deuda",
    "Comisión Mensual",
    "Apartado Mensual",
    "Fecha inicio",
    "DBT",
    "Deuda Resuelve",
    "Meses de atraso",
]

MESES_A_BUSCAR = 3


def get_credentials():
    info = json.loads(os.environ["MI_JSON"])
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
        ],
    )


creds = get_credentials()
drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)


def to_key(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        if v.is_integer():
            return str(int(v))
    return str(v).strip()


def shift_months(dt, m):
    y = dt.year + (dt.month - 1 + m) // 12
    mo = (dt.month - 1 + m) % 12 + 1
    return datetime(y, mo, 1)


def get_last_3():
    today = datetime.today()
    base = shift_months(datetime(today.year, today.month, 1), -1)
    return [shift_months(base, -i) for i in range(MESES_A_BUSCAR)]


MES = {
    1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
    5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",
    9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"
}


def sheet_name(dt):
    return f"{MES[dt.month]} {dt.year}"


def parse_range(name):
    m = re.search(r"([A-Za-z]{3})(\d{2})\s*-\s*([A-Za-z]{3})(\d{2})", name,re.I)
    if not m:
        return None
    map_={"ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
          "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12}
    return (
        2000+int(m.group(2)),
        map_[m.group(1).lower()],
        2000+int(m.group(4)),
        map_[m.group(3).lower()],
    )


def list_files():
    q=f"'{FOLDER_ID}' in parents and trashed=false and name contains 'Asignaciones de Cartera'"
    res=drive_service.files().list(q=q,fields="files(id,name,mimeType)").execute()
    out=[]
    for f in res["files"]:
        r=parse_range(f["name"])
        if r:
            f["range"]=r
            out.append(f)
    return out


def covers(r,dt):
    sy,sm,ey,em=r
    t=dt.year*12+dt.month
    a=sy*12+sm
    b=ey*12+em
    return a<=t<=b


def pick(files,dt):
    for f in files:
        if covers(f["range"],dt):
            return f
    raise Exception("no file")


def download(file):
    buf=io.BytesIO()
    if file["mimeType"]=="application/vnd.google-apps.spreadsheet":
        req=drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        req=drive_service.files().get_media(fileId=file["id"])

    d=MediaIoBaseDownload(buf,req)
    done=False
    while not done:
        _,done=d.next_chunk()

    buf.seek(0)
    return buf


def load():
    files=list_files()
    dfs=[]
    for m in get_last_3():
        try:
            f=pick(files,m)
            buf=download(f)
            df=pd.read_excel(buf,sheet_name=sheet_name(m))
            dfs.append(df)
        except:
            pass
    return pd.concat(dfs,ignore_index=True)


def read(range_):
    res=sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_
    ).execute()
    return res.get("values",[])


def append(rows):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{HOJA_DESTINO}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values":rows}
    ).execute()


def clean_value(v):
    if pd.isna(v):
        return ""
    if isinstance(v,pd.Timestamp):
        return v.strftime("%d/%m/%Y")
    return v


def main():

    refs={to_key(r[0]) for r in read(f"{HOJA_DATA}!A2:A") if r}
    ids={to_key(r[0]) for r in read(f"{HOJA_DESTINO}!B2:B") if r}

    df=load()
    df=df[COLUMNAS_NECESARIAS]

    df["ref"]=df["Referencia"].apply(to_key)
    df["id"]=df["Id deuda"].apply(to_key)

    df=df[df["ref"].isin(refs)]

    # id deuda unico
    df=df.drop_duplicates("id",keep="first")

    # excluir existentes
    df=df[~df["id"].isin(ids)]

    rows=[
        [clean_value(v) for v in row]
        for row in df[COLUMNAS_NECESARIAS].values
    ]

    if rows:
        append(rows)

    print("OK")


if __name__=="__main__":
    main()

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


# ======================================================
# CONFIG
# ======================================================
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

MES_MAP = {
    "ene": 1,"feb": 2,"mar": 3,"abr": 4,"may": 5,"jun": 6,
    "jul": 7,"ago": 8,"sep": 9,"oct": 10,"nov": 11,"dic": 12,
}

MES_NOMBRE = {
    1: "Enero",2: "Febrero",3: "Marzo",4: "Abril",
    5: "Mayo",6: "Junio",7: "Julio",8: "Agosto",
    9: "Septiembre",10: "Octubre",11: "Noviembre",12: "Diciembre",
}


# ======================================================
# AUTH
# ======================================================
def get_credentials() -> Credentials:
    mi_json = os.environ.get("MI_JSON")
    if not mi_json:
        raise ValueError("No se encontró MI_JSON")

    info: Dict = json.loads(mi_json)

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


# ======================================================
# HELPERS
# ======================================================
def to_key(value) -> str:
    if value is None:
        return ""

    if isinstance(value, float):
        if pd.isna(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return str(value).rstrip("0").rstrip(".")

    if isinstance(value, int):
        return str(value)

    s = str(value).strip()
    if not s:
        return ""

    if re.fullmatch(r"\d+\.0+", s):
        s = re.sub(r"\.0+$", "", s)

    return s


def shift_months(dt: datetime, delta: int) -> datetime:
    year = dt.year + (dt.month - 1 + delta) // 12
    month = (dt.month - 1 + delta) % 12 + 1
    return datetime(year, month, 1)


def get_last_n_months(n: int):
    today = datetime.today()
    base = shift_months(datetime(today.year, today.month, 1), -1)
    return [shift_months(base, -i) for i in range(n)]


def sheet_name_from_date(dt: datetime) -> str:
    return f"{MES_NOMBRE[dt.month]} {dt.year}"


# ======================================================
# DRIVE
# ======================================================
def parse_range_from_filename(name: str):
    m = re.search(r"([A-Za-z]{3})(\d{2})\s*-\s*([A-Za-z]{3})(\d{2})", name, re.I)
    if not m:
        return None

    m1, y1, m2, y2 = m.group(1).lower(), m.group(2), m.group(3).lower(), m.group(4)

    return (
        2000 + int(y1),
        MES_MAP[m1],
        2000 + int(y2),
        MES_MAP[m2],
    )


def list_files(folder_id):
    q = f"'{folder_id}' in parents and trashed=false and name contains 'Asignaciones de Cartera'"
    files = []
    page = None

    while True:
        resp = drive_service.files().list(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,modifiedTime)",
            pageToken=page,
        ).execute()

        for f in resp.get("files", []):
            fr = parse_range_from_filename(f["name"])
            if fr:
                f["parsed_range"] = fr
                files.append(f)

        page = resp.get("nextPageToken")
        if not page:
            break

    return files


def covers(range_, dt):
    sy, sm, ey, em = range_
    t = dt.year * 12 + dt.month
    a = sy * 12 + sm
    b = ey * 12 + em
    return a <= t <= b


def pick_file(files, dt):
    for f in files:
        if covers(f["parsed_range"], dt):
            return f
    raise Exception("archivo no encontrado")


def download_excel(file):
    buffer = io.BytesIO()

    if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
        req = drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        req = drive_service.files().get_media(fileId=file["id"])

    downloader = MediaIoBaseDownload(buffer, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer


def load_last_3_months():
    months = get_last_n_months(MESES_A_BUSCAR)
    files = list_files(FOLDER_ID)

    dfs = []

    for m in months:
        try:
            file = pick_file(files, m)
            buffer = download_excel(file)
            df = pd.read_excel(buffer, sheet_name=sheet_name_from_date(m))
            dfs.append(df)
        except:
            pass

    return pd.concat(dfs, ignore_index=True)


# ======================================================
# SHEETS
# ======================================================
def read_col(range_):
    res = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_,
    ).execute()

    return res.get("values", [])


def append_rows(rows):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{HOJA_DESTINO}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


# ======================================================
# MAIN
# ======================================================
def main():

    refs = {to_key(r[0]) for r in read_col(f"{HOJA_DATA}!A2:A") if r}

    ids_existentes = {
        to_key(r[0])
        for r in read_col(f"{HOJA_DESTINO}!B2:B")
        if r
    }

    df = load_last_3_months()

    df = df[COLUMNAS_NECESARIAS]

    df["ref"] = df["Referencia"].apply(to_key)
    df["id"] = df["Id deuda"].apply(to_key)

    df = df[df["ref"].isin(refs)]

    # id deuda unico
    df = df.drop_duplicates("id", keep="first")

    # excluir existentes
    df = df[~df["id"].isin(ids_existentes)]

    rows = df[COLUMNAS_NECESARIAS].values.tolist()

    if rows:
        append_rows(rows)

    print("ok")


if __name__ == "__main__":
    main()

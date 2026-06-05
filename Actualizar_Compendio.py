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

NOMBRE_ARCHIVO_CONTIENE = "Asignaciones de Cartera"


# ======================================================
# AUTH
# ======================================================

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


# ======================================================
# HELPERS
# ======================================================

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
    """
    Toma los 3 meses anteriores al mes actual.
    Si hoy es junio 2026:
    Mayo 2026, Abril 2026, Marzo 2026.
    """
    today = datetime.today()
    base = shift_months(datetime(today.year, today.month, 1), -1)
    return [shift_months(base, -i) for i in range(MESES_A_BUSCAR)]


MES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


MESES_MAP = {
    "ene": 1,
    "enero": 1,
    "feb": 2,
    "febrero": 2,
    "mar": 3,
    "marzo": 3,
    "abr": 4,
    "abril": 4,
    "may": 5,
    "mayo": 5,
    "jun": 6,
    "junio": 6,
    "jul": 7,
    "julio": 7,
    "ago": 8,
    "agosto": 8,
    "sep": 9,
    "sept": 9,
    "septiembre": 9,
    "oct": 10,
    "octubre": 10,
    "nov": 11,
    "noviembre": 11,
    "dic": 12,
    "diciembre": 12,
}


def limpiar_texto_fecha(x):
    x = str(x).strip().lower()
    x = (
        x.replace("á", "a")
         .replace("é", "e")
         .replace("í", "i")
         .replace("ó", "o")
         .replace("ú", "u")
    )
    return x


def sheet_name(dt):
    return f"{MES[dt.month]} {dt.year}"


def parse_mes_anio(texto):
    """
    Convierte:
    Ene25
    Abr25
    Abril25
    May26
    Ago26
    a datetime del primer día del mes.
    """
    if pd.isna(texto):
        return None

    t = limpiar_texto_fecha(texto)
    t = t.replace(" ", "")

    match = re.match(r"([a-zñ]+)(\d{2,4})", t)

    if not match:
        return None

    mes_txt = match.group(1)
    anio_txt = match.group(2)

    if mes_txt not in MESES_MAP:
        return None

    mes = MESES_MAP[mes_txt]

    if len(anio_txt) == 2:
        anio = 2000 + int(anio_txt)
    else:
        anio = int(anio_txt)

    return datetime(anio, mes, 1)


def parse_range(name):
    """
    Lee nombres como:
    Asignaciones de Cartera May26-Ago26.xlsx
    Asignaciones de Cartera Ene26-Abr26.xlsx
    Asignaciones de Cartera Ene25-Abr25.xlsx

    Devuelve:
    (año_inicio, mes_inicio, año_fin, mes_fin)
    """

    nombre = name

    # Quitar extensión si existe
    nombre = re.sub(r"\.xlsx$|\.xls$|\.xlsm$", "", nombre, flags=re.I)

    # Buscar patrón tipo May26-Ago26, Ene26-Abr26, Abril25-Jun25, etc.
    m = re.search(
        r"([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s*(\d{2,4})\s*-\s*([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s*(\d{2,4})",
        nombre,
        re.I
    )

    if not m:
        return None

    inicio_txt = f"{m.group(1)}{m.group(2)}"
    fin_txt = f"{m.group(3)}{m.group(4)}"

    fecha_ini = parse_mes_anio(inicio_txt)
    fecha_fin = parse_mes_anio(fin_txt)

    if fecha_ini is None or fecha_fin is None:
        return None

    return (
        fecha_ini.year,
        fecha_ini.month,
        fecha_fin.year,
        fecha_fin.month,
    )


def covers(r, dt):
    sy, sm, ey, em = r

    t = dt.year * 12 + dt.month
    a = sy * 12 + sm
    b = ey * 12 + em

    return a <= t <= b


# ======================================================
# BUSCAR ARCHIVOS EN DRIVE
# ======================================================

def list_files():
    q = (
        f"'{FOLDER_ID}' in parents "
        "and trashed=false "
        f"and name contains '{NOMBRE_ARCHIVO_CONTIENE}'"
    )

    files = []
    page_token = None

    while True:
        res = drive_service.files().list(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,modifiedTime)",
            pageSize=100,
            pageToken=page_token
        ).execute()

        files.extend(res.get("files", []))

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    out = []

    print("📌 Archivos encontrados en la carpeta:")

    for f in files:
        r = parse_range(f["name"])

        if r:
            f["range"] = r
            out.append(f)

            sy, sm, ey, em = r
            print(
                f" - {f['name']} | "
                f"{MES[sm]} {sy} a {MES[em]} {ey}"
            )
        else:
            print(f"⚠️ Ignorado, no pude leer rango de fechas: {f['name']}")

    if not out:
        raise FileNotFoundError(
            f"No encontré archivos válidos con nombre que contenga: {NOMBRE_ARCHIVO_CONTIENE}"
        )

    return out


def pick(files, dt):
    candidatos = [f for f in files if covers(f["range"], dt)]

    if not candidatos:
        raise FileNotFoundError(
            f"No encontré archivo que cubra el mes: {sheet_name(dt)}"
        )

    # Si hay más de uno, toma el de modificación más reciente
    candidatos = sorted(
        candidatos,
        key=lambda x: x.get("modifiedTime", ""),
        reverse=True
    )

    return candidatos[0]


def download(file):
    buf = io.BytesIO()

    if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
        req = drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        req = drive_service.files().get_media(fileId=file["id"])

    d = MediaIoBaseDownload(buf, req)

    done = False
    while not done:
        _, done = d.next_chunk()

    buf.seek(0)
    return buf


# ======================================================
# CARGAR LOS 3 MESES MÁS RECIENTES
# ======================================================

def load():
    files = list_files()
    meses = get_last_3()

    print("\n📅 Meses que se van a buscar:")

    for m in meses:
        print(f" - {sheet_name(m)}")

    dfs = []
    cache_archivos = {}

    for m in meses:
        nombre_hoja = sheet_name(m)

        try:
            f = pick(files, m)

            print(f"\n📄 Mes: {nombre_hoja}")
            print(f"   Archivo seleccionado: {f['name']}")

            if f["id"] not in cache_archivos:
                cache_archivos[f["id"]] = download(f)

            buf = cache_archivos[f["id"]]
            buf.seek(0)

            df = pd.read_excel(buf, sheet_name=nombre_hoja)

            df["archivo_origen"] = f["name"]
            df["hoja_origen"] = nombre_hoja

            print(f"   Filas leídas: {len(df):,}")

            dfs.append(df)

        except Exception as e:
            print(f"⚠️ No se pudo leer {nombre_hoja}: {e}")

    if not dfs:
        raise ValueError("No se pudo cargar información de ningún mes.")

    return pd.concat(dfs, ignore_index=True)


# ======================================================
# GOOGLE SHEETS DESTINO
# ======================================================

def read(range_):
    res = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_
    ).execute()

    return res.get("values", [])


def append(rows):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{HOJA_DESTINO}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


def clean_value(v):
    if pd.isna(v):
        return ""

    if isinstance(v, pd.Timestamp):
        return v.strftime("%d/%m/%Y")

    if isinstance(v, datetime):
        return v.strftime("%d/%m/%Y")

    return v


# ======================================================
# MAIN
# ======================================================

def main():

    refs = {
        to_key(r[0])
        for r in read(f"{HOJA_DATA}!A2:A")
        if r
    }

    ids = {
        to_key(r[0])
        for r in read(f"{HOJA_DESTINO}!B2:B")
        if r
    }

    print(f"\n🔎 Referencias en {HOJA_DATA}!A2:A: {len(refs):,}")
    print(f"🔎 Id deuda existentes en {HOJA_DESTINO}!B2:B: {len(ids):,}")

    df = load()

    print(f"\n📊 Filas cargadas antes de filtros: {len(df):,}")

    # Normalizar nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in COLUMNAS_NECESARIAS if c not in df.columns]

    if missing:
        raise ValueError(
            f"Faltan estas columnas en los archivos origen: {missing}"
        )

    df = df[COLUMNAS_NECESARIAS].copy()

    df["ref"] = df["Referencia"].apply(to_key)
    df["id"] = df["Id deuda"].apply(to_key)

    # Filtrar solo referencias que están en Data
    df = df[df["ref"].isin(refs)].copy()

    print(f"📊 Filas después de filtrar referencias de Data: {len(df):,}")

    # Id deuda único dentro de lo cargado
    df = df.drop_duplicates("id", keep="first").copy()

    print(f"📊 Filas después de quitar duplicados por Id deuda: {len(df):,}")

    # Excluir los Id deuda que ya están en Cartera mes anterior
    df = df[~df["id"].isin(ids)].copy()

    print(f"📊 Filas nuevas para agregar: {len(df):,}")

    rows = [
        [clean_value(v) for v in row]
        for row in df[COLUMNAS_NECESARIAS].values
    ]

    if rows:
        append(rows)
        print(f"\n✅ OK. Se agregaron {len(rows):,} filas nuevas.")
    else:
        print("\n✅ OK. No había filas nuevas para agregar.")


if __name__ == "__main__":
    main()

import io
import json
import os
import re
from datetime import datetime

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

NOMBRE_ARCHIVO_CONTIENE = "Asignaciones de Cartera"

# Últimos 3 meses anteriores al mes actual
MESES_A_BUSCAR = 3

# Columna de referencia en Data
COLUMNA_REFERENCIA_DATA = "Referencia"

# Columnas que se van a buscar en los archivos de asignaciones
COLUMNAS_A_TRAER = [
    "Referencia",
    "Id deuda",
    "Comisión Mensual",
    "Apartado Mensual",
    "Fecha inicio",
    "DBT",
    "Deuda Resuelve",
    "Meses de atraso",
]


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
# HELPERS GENERALES
# ======================================================

def to_key(v):
    if v is None:
        return ""

    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass

    txt = str(v).strip()

    # Corrige valores que vienen como 123456.0
    if txt.endswith(".0"):
        txt = txt[:-2]

    # Quita espacios raros
    txt = txt.replace("\u00A0", "").strip()

    return txt


def clean_value(v):
    if pd.isna(v):
        return ""

    if isinstance(v, pd.Timestamp):
        return v.strftime("%d/%m/%Y")

    if isinstance(v, datetime):
        return v.strftime("%d/%m/%Y")

    return v


def normalizar_columnas(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def read_sheet_values(range_):
    res = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_
    ).execute()

    return res.get("values", [])


def append(rows):
    """
    Agrega filas abajo en la hoja destino.
    No borra nada.
    """
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{HOJA_DESTINO}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


# ======================================================
# LEER GOOGLE SHEET COMO DATAFRAME
# ======================================================

def leer_google_sheet_como_df(nombre_hoja):
    values = read_sheet_values(f"{nombre_hoja}!A:Z")

    if not values:
        return pd.DataFrame()

    header = values[0]
    data = values[1:]

    data_ajustada = []

    for row in data:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[:len(header)]

        data_ajustada.append(row)

    df = pd.DataFrame(data_ajustada, columns=header)
    df = normalizar_columnas(df)

    return df


def leer_referencias_data():
    df_data = leer_google_sheet_como_df(HOJA_DATA)

    if df_data.empty:
        raise ValueError(f"La hoja '{HOJA_DATA}' está vacía o no se pudo leer.")

    print(f"📥 Filas leídas desde {HOJA_DATA}: {len(df_data):,}")
    print("Columnas encontradas en Data:")
    print(df_data.columns.tolist())

    if COLUMNA_REFERENCIA_DATA not in df_data.columns:
        raise ValueError(
            f"No encontré la columna '{COLUMNA_REFERENCIA_DATA}' en la hoja '{HOJA_DATA}'. "
            f"Columnas encontradas: {df_data.columns.tolist()}"
        )

    refs = {
        to_key(x)
        for x in df_data[COLUMNA_REFERENCIA_DATA]
        if to_key(x) != ""
    }

    print(f"🔎 Referencias únicas encontradas en {HOJA_DATA}: {len(refs):,}")

    return refs


# ======================================================
# FECHAS Y MESES
# ======================================================

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


def parse_mes_anio(texto):
    """
    Convierte textos como:
    Ene25, Abril25, May26, Ago26
    a datetime del primer día del mes.
    """
    if pd.isna(texto):
        return None

    t = limpiar_texto_fecha(texto)
    t = t.replace(" ", "")

    m = re.match(r"([a-zñ]+)(\d{2,4})", t)

    if not m:
        return None

    mes_txt = m.group(1)
    anio_txt = m.group(2)

    if mes_txt not in MESES_MAP:
        return None

    mes = MESES_MAP[mes_txt]

    if len(anio_txt) == 2:
        anio = 2000 + int(anio_txt)
    else:
        anio = int(anio_txt)

    return datetime(anio, mes, 1)


def shift_months(dt, m):
    y = dt.year + (dt.month - 1 + m) // 12
    mo = (dt.month - 1 + m) % 12 + 1
    return datetime(y, mo, 1)


def get_last_3():
    """
    Si hoy es junio 2026, devuelve:
    Mayo 2026, Abril 2026, Marzo 2026.
    """
    today = datetime.today()
    base = shift_months(datetime(today.year, today.month, 1), -1)
    return [shift_months(base, -i) for i in range(MESES_A_BUSCAR)]


def sheet_name(dt):
    return f"{MES[dt.month]} {dt.year}"


def parse_range(name):
    """
    Lee nombres como:
    Asignaciones de Cartera May26-Ago26.xlsx
    Asignaciones de Cartera Ene26-Abr26.xlsx
    Asignaciones de Cartera Sep25-Dic25.xlsx
    """

    nombre = re.sub(r"\.xlsx$|\.xls$|\.xlsm$", "", name, flags=re.I)

    m = re.search(
        r"([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s*(\d{2,4})\s*-\s*([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s*(\d{2,4})",
        nombre,
        re.I
    )

    if not m:
        return None

    fecha_ini = parse_mes_anio(f"{m.group(1)}{m.group(2)}")
    fecha_fin = parse_mes_anio(f"{m.group(3)}{m.group(4)}")

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
# BUSCAR ARCHIVOS DE ASIGNACIONES EN DRIVE
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

    print("\n📌 Archivos encontrados en Drive:")

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
        raise FileNotFoundError(f"No encontré archivo para el mes {sheet_name(dt)}")

    # Si hay más de uno para el mismo mes, toma el más recientemente modificado
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

    downloader = MediaIoBaseDownload(buf, req)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buf.seek(0)

    return buf


# ======================================================
# LECTURA DE EXCELS DE ASIGNACIONES
# ======================================================

def leer_hoja_excel_con_header_flexible(buf, sheet):
    """
    Detecta la fila de encabezados.
    Sirve si la hoja trae encabezado en fila 1, fila 2, etc.
    Busca una fila donde existan 'Referencia' e 'Id deuda'.
    """

    buf.seek(0)

    preview = pd.read_excel(
        buf,
        sheet_name=sheet,
        header=None,
        nrows=15
    )

    header_row = None

    for idx in range(len(preview)):
        valores = [
            str(x).strip()
            for x in preview.iloc[idx].tolist()
            if str(x).strip().lower() != "nan"
        ]

        if "Referencia" in valores and "Id deuda" in valores:
            header_row = idx
            break

    buf.seek(0)

    if header_row is None:
        print(f"⚠️ No detecté encabezado flexible en hoja {sheet}. Intento con header=0.")
        df = pd.read_excel(buf, sheet_name=sheet)
    else:
        df = pd.read_excel(buf, sheet_name=sheet, header=header_row)

    df = normalizar_columnas(df)

    return df


def load_asignaciones():
    files = list_files()
    meses = get_last_3()

    print("\n📅 Meses que se van a buscar en archivos de asignaciones:")

    for m in meses:
        print(f" - {sheet_name(m)}")

    dfs = []
    cache_archivos = {}

    for m in meses:
        nombre_hoja = sheet_name(m)

        try:
            f = pick(files, m)

            print(f"\n📄 Buscando mes: {nombre_hoja}")
            print(f"   Archivo seleccionado: {f['name']}")

            if f["id"] not in cache_archivos:
                cache_archivos[f["id"]] = download(f)

            buf = cache_archivos[f["id"]]
            buf.seek(0)

            df = leer_hoja_excel_con_header_flexible(buf, nombre_hoja)

            df["archivo_origen"] = f["name"]
            df["hoja_origen"] = nombre_hoja

            print(f"   Filas leídas: {len(df):,}")
            print(f"   Columnas: {df.columns.tolist()}")

            dfs.append(df)

        except Exception as e:
            print(f"⚠️ No se pudo leer {nombre_hoja}: {e}")

    if not dfs:
        raise ValueError("No se pudo leer ningún archivo de asignaciones.")

    df_final = pd.concat(dfs, ignore_index=True)

    return df_final


# ======================================================
# MAIN
# ======================================================

def main():

    print("====================================================")
    print("INICIO PROCESO CARTERA MES ANTERIOR")
    print("====================================================")

    # 1. Leer referencias desde Data
    refs_data = leer_referencias_data()

    # 2. Leer referencias ya existentes en Cartera mes anterior
    # En destino, Referencia está en columna A
    refs_existentes_destino = {
        to_key(r[0])
        for r in read_sheet_values(f"{HOJA_DESTINO}!A2:A")
        if r
    }

    print(f"\n🔎 Referencias ya existentes en '{HOJA_DESTINO}': {len(refs_existentes_destino):,}")

    # 3. De Data solo nos interesan las que aún NO están en Cartera mes anterior
    refs_pendientes = refs_data - refs_existentes_destino

    print(f"📌 Referencias de Data pendientes por buscar/agregar: {len(refs_pendientes):,}")

    if not refs_pendientes:
        print(f"\n✅ OK. Todas las referencias de Data ya existen en '{HOJA_DESTINO}'.")
        return

    # 4. Leer asignaciones desde Drive
    df_asig = load_asignaciones()

    print(f"\n📊 Total filas cargadas desde asignaciones: {len(df_asig):,}")

    # 5. Validar columnas necesarias en archivos de asignaciones
    missing = [c for c in COLUMNAS_A_TRAER if c not in df_asig.columns]

    if missing:
        raise ValueError(
            f"Faltan estas columnas en los archivos de asignaciones: {missing}. "
            f"Columnas encontradas: {df_asig.columns.tolist()}"
        )

    # 6. Tomar solo columnas necesarias
    df = df_asig[COLUMNAS_A_TRAER].copy()

    # 7. Normalizar llaves
    df["Referencia_key"] = df["Referencia"].apply(to_key)

    # 8. Filtrar solo referencias pendientes
    df = df[df["Referencia_key"].isin(refs_pendientes)].copy()

    print(f"\n📊 Filas encontradas en asignaciones que cruzan con pendientes: {len(df):,}")

    # 9. Diagnóstico de referencias encontradas/no encontradas
    refs_encontradas = set(df["Referencia_key"])
    refs_no_encontradas = refs_pendientes - refs_encontradas

    print(f"🔎 Referencias pendientes encontradas en asignaciones: {len(refs_encontradas):,}")
    print(f"⚠️ Referencias pendientes NO encontradas en últimos {MESES_A_BUSCAR} meses: {len(refs_no_encontradas):,}")

    if refs_no_encontradas:
        print("Ejemplo de referencias NO encontradas:")
        print(list(refs_no_encontradas)[:30])

    if df.empty:
        print(f"\n✅ OK. No se encontraron referencias nuevas para agregar en '{HOJA_DESTINO}'.")
        return

    # 10. Quitar filas sin referencia
    df = df[df["Referencia_key"] != ""].copy()

    # 11. Quitar duplicados por Referencia
    # Si una referencia aparece varias veces, deja la primera encontrada.
    df = df.drop_duplicates(subset=["Referencia_key"], keep="first").copy()

    print(f"📊 Filas después de quitar duplicados por Referencia: {len(df):,}")

    # 12. Seguridad adicional: volver a excluir referencias ya existentes en destino
    df = df[~df["Referencia_key"].isin(refs_existentes_destino)].copy()

    print(f"📊 Referencias finales nuevas para agregar: {len(df):,}")

    if df.empty:
        print(f"\n✅ OK. Después de validar existentes, no quedó nada nuevo para agregar.")
        return

    # 13. Preparar filas finales con las columnas exactas
    rows = [
        [clean_value(v) for v in row]
        for row in df[COLUMNAS_A_TRAER].values
    ]

    # 14. Agregar abajo en Cartera mes anterior
    append(rows)

    print(f"\n✅ OK. Se agregaron {len(rows):,} referencias nuevas en '{HOJA_DESTINO}'.")
    print("No se borró ninguna fila existente.")

    print("====================================================")
    print("FIN PROCESO")
    print("====================================================")


if __name__ == "__main__":
    main()

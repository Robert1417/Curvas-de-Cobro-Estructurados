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

MES_MAP = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

MES_NOMBRE = {
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


# ======================================================
# AUTH
# ======================================================
def get_credentials() -> Credentials:
    mi_json = os.environ.get("MI_JSON")
    if not mi_json:
        raise ValueError("No se encontró MI_JSON en variables de entorno")

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
# HELPERS DE TEXTO / FECHAS
# ======================================================
def normalizar_texto(txt: str) -> str:
    if txt is None:
        return ""
    txt = str(txt).strip().lower()
    replacements = str.maketrans(
        "áéíóúüñ",
        "aeiouun",
    )
    txt = txt.translate(replacements)
    txt = re.sub(r"\s+", " ", txt)
    return txt


def to_key(value) -> str:
    """
    Normaliza referencias para comparar sin problemas de tipo:
    123, "123", 123.0, "123.0" => "123"
    """
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


def get_previous_month_date() -> datetime:
    today = datetime.today()
    year = today.year
    month = today.month - 1

    if month == 0:
        month = 12
        year -= 1

    return datetime(year, month, 1)


def sheet_name_from_date(dt: datetime) -> str:
    return f"{MES_NOMBRE[dt.month]} {dt.year}"


def month_index(year: int, month: int) -> int:
    return year * 12 + month


# ======================================================
# DRIVE: ARCHIVOS ORIGEN
# ======================================================
def parse_range_from_filename(name: str) -> Optional[Tuple[int, int, int, int]]:
    """
    Ejemplo:
    Asignaciones de Cartera Ene26-Abr26.xlsx
    """
    m = re.search(r"([A-Za-z]{3})(\d{2})\s*-\s*([A-Za-z]{3})(\d{2})", name, flags=re.IGNORECASE)
    if not m:
        return None

    m1, y1, m2, y2 = m.group(1).lower(), m.group(2), m.group(3).lower(), m.group(4)

    if m1 not in MES_MAP or m2 not in MES_MAP:
        return None

    return (2000 + int(y1), MES_MAP[m1], 2000 + int(y2), MES_MAP[m2])


def file_covers_month(file_range: Tuple[int, int, int, int], target_dt: datetime) -> bool:
    sy, sm, ey, em = file_range
    t = month_index(target_dt.year, target_dt.month)
    a = month_index(sy, sm)
    b = month_index(ey, em)
    return a <= t <= b


def list_assignment_files_in_folder(folder_id: str) -> List[Dict]:
    q = f"'{folder_id}' in parents and trashed=false and name contains 'Asignaciones de Cartera'"
    files: List[Dict] = []
    page_token = None

    while True:
        resp = (
            drive_service.files()
            .list(
                q=q,
                fields="nextPageToken, files(id,name,mimeType,modifiedTime)",
                pageToken=page_token,
            )
            .execute()
        )

        for f in resp.get("files", []):
            fr = parse_range_from_filename(f.get("name", ""))
            if fr:
                f["parsed_range"] = fr
                files.append(f)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if not files:
        raise ValueError("No encontré archivos 'Asignaciones de Cartera' válidos en la carpeta")

    return files


def pick_file_for_month(files_meta: List[Dict], target_dt: datetime) -> Dict:
    candidates = []
    for f in files_meta:
        fr = f["parsed_range"]
        if file_covers_month(fr, target_dt):
            sy, sm, ey, em = fr
            span = month_index(ey, em) - month_index(sy, sm)
            candidates.append((span, f.get("modifiedTime", ""), f))

    if not candidates:
        raise ValueError(f"No encontré archivo que cubra el mes {sheet_name_from_date(target_dt)}")

    candidates.sort(key=lambda x: (x[0], x[1]))
    min_span = candidates[0][0]
    same_span = [c for c in candidates if c[0] == min_span]
    same_span.sort(key=lambda x: x[1], reverse=True)
    return same_span[0][2]


def download_file_to_buffer(file_id: str, mime_type: str) -> io.BytesIO:
    buffer = io.BytesIO()

    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive_service.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer


def load_previous_month_source_df(folder_id: str) -> pd.DataFrame:
    target_dt = get_previous_month_date()
    target_sheet = sheet_name_from_date(target_dt)

    files_meta = list_assignment_files_in_folder(folder_id)
    selected_file = pick_file_for_month(files_meta, target_dt)

    print(f"Archivo elegido: {selected_file['name']}")
    print(f"Hoja objetivo: {target_sheet}")

    buffer = download_file_to_buffer(selected_file["id"], selected_file["mimeType"])
    df = pd.read_excel(buffer, sheet_name=target_sheet, engine="openpyxl")

    if df.shape[0] == 0:
        raise ValueError(f"La hoja {target_sheet} está vacía")

    return df


# ======================================================
# SHEETS: LEER / ESCRIBIR
# ======================================================
def get_sheet_values(spreadsheet_id: str, range_a1: str) -> List[List]:
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_a1)
        .execute()
    )
    return result.get("values", [])


def ensure_sheet_exists_with_headers(spreadsheet_id: str, sheet_name: str, headers: List[str]) -> None:
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    existing_names = [s["properties"]["title"] for s in sheets]

    if sheet_name not in existing_names:
        requests = [{
            "addSheet": {
                "properties": {
                    "title": sheet_name
                }
            }
        }]
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    current = get_sheet_values(spreadsheet_id, f"'{sheet_name}'!1:1")
    if not current or current[0] != headers:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()


def append_rows(spreadsheet_id: str, sheet_name: str, rows: List[List]) -> None:
    if not rows:
        return

    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def obtener_referencias_data(spreadsheet_id: str, hoja_data: str) -> set:
    values = get_sheet_values(spreadsheet_id, f"'{hoja_data}'!A2:A")
    referencias = set()

    for row in values:
        if not row:
            continue
        key = to_key(row[0])
        if key:
            referencias.add(key)

    return referencias


def obtener_referencias_existentes_destino(spreadsheet_id: str, hoja_destino: str) -> set:
    values = get_sheet_values(spreadsheet_id, f"'{hoja_destino}'!A2:A")
    existentes = set()

    for row in values:
        if not row:
            continue
        key = to_key(row[0])
        if key:
            existentes.add(key)

    return existentes


# ======================================================
# DATAFRAME: EXTRAER Y FILTRAR
# ======================================================
def select_required_columns(df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
    df_cols_normalized = {normalizar_texto(c): c for c in df.columns}
    selected_real_columns = []

    for req in required_columns:
        req_norm = normalizar_texto(req)
        if req_norm not in df_cols_normalized:
            raise ValueError(f"No encontré la columna requerida: {req}")
        selected_real_columns.append(df_cols_normalized[req_norm])

    result = df[selected_real_columns].copy()
    result.columns = required_columns
    return result


def preparar_filas_nuevas(df_source: pd.DataFrame, referencias_data: set, referencias_destino: set) -> List[List]:
    df = select_required_columns(df_source, COLUMNAS_NECESARIAS).copy()

    df["__ref_key__"] = df["Referencia"].apply(to_key)
    df = df[df["__ref_key__"] != ""]
    df = df[df["__ref_key__"].isin(referencias_data)]
    df = df[~df["__ref_key__"].isin(referencias_destino)]

    if df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        values = []
        for col in COLUMNAS_NECESARIAS:
            val = row[col]

            if pd.isna(val):
                values.append("")
            elif isinstance(val, pd.Timestamp):
                values.append(val.strftime("%d/%m/%Y"))
            else:
                values.append(val)

        rows.append(values)

    return rows


# ======================================================
# MAIN
# ======================================================
def main():
    print("Iniciando proceso...")

    referencias_data = obtener_referencias_data(SPREADSHEET_ID, HOJA_DATA)
    print(f"Referencias en Data: {len(referencias_data)}")

    ensure_sheet_exists_with_headers(SPREADSHEET_ID, HOJA_DESTINO, COLUMNAS_NECESARIAS)

    referencias_destino = obtener_referencias_existentes_destino(SPREADSHEET_ID, HOJA_DESTINO)
    print(f"Referencias ya existentes en '{HOJA_DESTINO}': {len(referencias_destino)}")

    df_source = load_previous_month_source_df(FOLDER_ID)

    nuevas_filas = preparar_filas_nuevas(
        df_source=df_source,
        referencias_data=referencias_data,
        referencias_destino=referencias_destino,
    )

    print(f"Nuevas filas a agregar: {len(nuevas_filas)}")

    if nuevas_filas:
        append_rows(SPREADSHEET_ID, HOJA_DESTINO, nuevas_filas)
        print("Filas agregadas correctamente.")
    else:
        print("No hay filas nuevas para agregar.")

    print("Proceso terminado.")


if __name__ == "__main__":
    main()

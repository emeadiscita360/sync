import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import logging
import sys

# --- Setup Logging ---
log_file = "log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True  # Python 3.8+: forces reconfiguration if logging is already set
)

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

# --- Start Logging ---
print("🟡 Script is starting...")  # Will show in Actions live log
logging.info("Script started.")

try:
    # --- CONFIG ---
    SQL_SERVER = 'USSTLIAPBINDB03.emrsn.org'
    DATABASE = 'GlobalMaster'

    # --- Connection Setup ---
    conn_str = (
        f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}"
        "?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(conn_str)

    # --- Load Filter File ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filter_path = os.path.join(script_dir, "MaraExtract_Filter.txt")
    if not os.path.exists(filter_path):
        raise FileNotFoundError(f"Missing filter file: {filter_path}")

    filter_df = pd.read_csv(filter_path, sep="\t", dtype=str)
    allowed_items = set(filter_df["MATNR"].str.strip())

    # --- SQL Query ---
    query = """
    WITH base AS (
        SELECT 
            gm.ItemNumber,
            gm.CommercialCode,
            gm.Organization,
            gm.ItemStatus,
            gm.TopSales
        FROM GlobalMaster.dbo.GlobalItemMaster_new gm
        WHERE gm.Organization IN ('ASCO_EUROMASTER', '7290')
    )
    SELECT 
        a.ItemNumber,
        a.CommercialCode,
        a.ItemStatus AS ASCO_Status,
        a.TopSales,
        s.ItemStatus AS SAP_Status,
        s.ItemNumber AS SAP_ItemNumber
    FROM base a
    JOIN base s 
        ON a.CommercialCode = s.CommercialCode
    WHERE a.Organization = 'ASCO_EUROMASTER'
      AND s.Organization = '7290'
      AND ISNULL(a.TopSales, 0) = 0
      AND ISNULL(s.ItemStatus, '') <> 'AE'
    GROUP BY 
        a.ItemNumber, 
        a.CommercialCode, 
        a.ItemStatus, 
        a.TopSales, 
        s.ItemStatus, 
        s.ItemNumber;
    """

    # --- Run Query ---
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    # --- Filter based on MaraExtract ---
    df["SAP_ItemNumber"] = df["SAP_ItemNumber"].astype(str).str.strip()
    df = df[df["SAP_ItemNumber"].isin(allowed_items)]

    # --- Generate Filenames ---
    now = datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    excel_file = f"ItemStatusDiscrepancy_{timestamp}.xlsx"
    workfiles_folder = os.path.join(script_dir, "WorkFiles")
    os.makedirs(workfiles_folder, exist_ok=True)
    txt_file = os.path.join(workfiles_folder, f"ItemStatusDiscrepancy_{timestamp}.txt")

    # --- Status Mapping ---
    status_mapping = {
        "E-ACTIVE": "AV",
        "E-ACTIVE NON-SEL": "AN",
        "E-DESIGN": "EW",
        "E-INACTIVE": "AE",
        "E-PHASE-OUT": "AL"
    }

    # --- TXT Headers ---
    txt_headers = [...]  # same as your current list

    # --- Prepare TXT Content ---
    iv_text_de = f"{now.strftime('%d.%m.%Y')}: Item Status modified | Syteline-SAP syncronisation |"
    iv_text_en = f"{now.strftime('%Y-%m-%d')}: Item Status modified | Syteline-SAP syncronisation |"

    txt_rows = []
    for _, row_df in df.iterrows():
        row = {header: "" for header in txt_headers}
        row["MARA-MATNR"] = row_df["SAP_ItemNumber"]
        row["IV-TEXT_DE"] = iv_text_de
        row["IV-TEXT_EN"] = iv_text_en
        row["MARA-BEGRU"] = "4000"
        row["MARC-MMSTA"] = "AE"
        row["MARA-ZZRCL"] = "QC01"
        txt_rows.append(row)

    # --- Save TXT ---
    df_txt = pd.DataFrame(txt_rows, columns=txt_headers)
    df_txt.to_csv(txt_file, sep="\t", index=False)

    logging.info(f"✅ Saved TXT file: {txt_file}")
    logging.info("Script finished successfully.")

except Exception as e:
    logging.exception("❌ An error occurred during script execution.")
    print(f"❌ Script failed: {e}")
    sys.exit(1)

# Ensure all logs are flushed
for handler in logging.getLogger("").handlers:
    handler.flush()

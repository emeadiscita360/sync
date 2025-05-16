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
    format="%(asctime)s [%(levelname)s] %(message)s"
)

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

# --- Start Logging ---
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
    logging.info(f"✅ SQL rows returned: {len(df)}")

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
    txt_headers = [
        "MARA-MATNR", "MARA-MTART", "MARA-YYGTYPKURZB", "MARA-ZZBCODE", "MARA-ZZUPDKT",
        "GD-TEXT_DE", "GD-TEXT_EN", "GD-TEXT_FR", "GD-TEXT_IT", "GD-TEXT_NL",
        "GD-TEXT_PT", "GD-TEXT_ES", "GD-TEXT_FI", "GD-TEXT_SV", "IV-TEXT_DE",
        "IV-TEXT_EN", "IV-TEXT_FR", "IV-TEXT_IT", "IV-TEXT_NL", "IV-TEXT_PT",
        "IV-TEXT_ES", "IV-TEXT_FI", "IV-TEXT_SV", "MARA-MEINS", "MARA-SPART",
        "MARA-MATKL", "MARA-LABOR", "MARA-BISMT", "MARA-ZZNEUMT", "MARA-ZZBOSCHMAT",
        "MARA-PRDHA", "MARA-MTPOS_MARA", "MARA-ZZVORZTYP", "MARA-BEGRU", "MARC-MMSTA",
        "MARC-MMSTD", "MARA-ZZVWERK", "MARA-BRGEW", "MARA-NTGEW", "MARA-GEWEI",
        "MARA-VOLUM", "MARA-VOLEH", "MARA-GROES", "MARA-NORMT", "MARA-WRKST",
        "MARA-KZUMW", "MARA-YYGEXPDAT", "MARA-ILOOS", "MARA-ZEINR", "MARA-ZEIAR",
        "MARA-ZEIVR", "MARA-BLATT", "MARA-AESZN", "MARA-ZEIFO", "MARA-BLANZ",
        "MVKE-AUMNG", "MVKE-LFMNG", "MVKE-SCMNG", "MVKE-MVGR3", "MARA-ZZRCL",
        "MVKE-SCHME", "MARC-HERKL", "MARC-HERKR", "MARC-STAWN", "MARC-MAXLZ",
        "MARC-LZEIH", "MARC-ZZLOGKL", "MARA-YYGPARTCAT1", "MARA-YYGPARTCAT2",
        "MATNR_ASCO", "CCODE_ASCO"
    ]

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
    logging.info(f"✅ Filtered item count: {len(df)}")
    logging.info(f"✅ WorkFiles directory path: {workfiles_folder}")
    logging.info(f"✅ Output TXT will be saved as: {txt_file}")
    logging.info(f"✅ Saved TXT file: {txt_file}")
    logging.info("Script finished successfully.")

except Exception as e:
    logging.exception("An error occurred during script execution.")
    sys.exit(1)

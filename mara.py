import os
import time
import pandas as pd
from datetime import datetime

# CONFIG
file_path = r"\\emrsn.org\dfsiap\ECF\Public\BOM-Analyzer\Input-files\MARA\MARA.TXT"  
last_check_file = "mara_last_check.txt"
output_file = "MaraExtract_Filter.txt"
log_file = "log.txt"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(full_message + "\n")

def get_file_creation_time(path):
    try:
        return os.path.getctime(path)
    except Exception as e:
        log(f"Error accessing mara file creation time: {e}")
        return 0.0

def get_last_check_time():
    if os.path.exists(last_check_file):
        try:
            with open(last_check_file, "r") as f:
                return float(f.read().strip())
        except Exception as e:
            log(f"Error reading last check time of mara table: {e}")
            return 0.0
    return 0.0

def update_last_check_time(timestamp):
    try:
        with open(last_check_file, "w") as f:
            f.write(str(timestamp))
    except Exception as e:
        log(f"Error updating mara last check time: {e}")

def process_file(file_path):
    log("Reading mara file and extracting columns...")
    try:
        df = pd.read_csv(
            file_path,
            sep="\t",
            header=0,
            usecols=[0, 5],  # Only read 1st and 6th columns
            names=["MATNR", "ZZVWERK"],
            engine="python",
            on_bad_lines="skip"  # Skip malformed rows
        )
        log(f"Mara file read successfully. Total rows read: {len(df)}")
    except Exception as e:
        log(f"Failed to read mara: {e}")
        return

    # Drop duplicates
    df_unique = df.drop_duplicates(subset=["MATNR", "ZZVWERK"])
    log(f"Unique rows retained: {len(df_unique)}")

    # Filter where ZZVWERK == "ASCO"
    df_filtered = df_unique[df_unique["ZZVWERK"] == "ASCO"]
    log(f"Rows after filtering for ZZVWERK == 'ASCO': {len(df_filtered)}")

    # Save to output
    try:
        df_filtered.to_csv(output_file, sep="\t", index=False)
        log(f"Filtered data written to: {output_file}")
    except Exception as e:
        log(f"Error writing output file: {e}")

def main():
    log("Starting processing script...")

    creation_time = get_file_creation_time(file_path)
    last_check = get_last_check_time()

    if creation_time > last_check:
        log("New file detected. Processing...")
        process_file(file_path)
        update_last_check_time(creation_time)
        log("Processing complete.\n")
    else:
        log("No updates since last check. Skipping.\n")

if __name__ == "__main__":
    main()
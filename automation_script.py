import subprocess
import datetime
import os
import logging
from calendar import monthrange # Not used, but kept from original code

# --- Configuration ---
# PROCESSED_LIST_FILE = "/home/eouser/clms/config/processed_input_files.txt"
# LOG_FILE = "/home/eouser/clms/logs/clipper_automation.log" # Common log file
PROCESSED_LIST_FILE = "/data/processed_input_files.txt"
LOG_FILE = "/data/clipper_automation.log" # Common log file
# --- Logging Setup ---
def setup_logging():
    """Configures the logging system to output to a file and the console."""
    logging.basicConfig(
        level=logging.INFO, # Set base level to INFO
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            # Handler to write logs to a common file
            logging.FileHandler(LOG_FILE),
            # Handler to output logs to the console
            logging.StreamHandler()
        ]
    )

setup_logging()
# --- End Logging Setup ---

# --- Utility Functions ---

def load_processed_list(file_path):
    """Loads the set of processed filenames from a file."""
    if not os.path.exists(file_path):
        logging.info(f"Processed list file not found at {file_path}. Starting with empty list.")
        return set()
    try:
        with open(file_path, 'r') as f:
            processed = set(line.strip() for line in f if line.strip())
            logging.debug(f"Successfully loaded {len(processed)} previously processed files from {file_path}.")
            return processed
    except IOError as e:
        logging.error(f"Failed to load processed list from {file_path}: {e}")
        return set()

def write_to_processed_list(file_path, filename):
    """Appends a new filename to the processed list file."""
    try:
        with open(file_path, 'a') as f:
            f.write(filename + '\n')
        logging.info(f"Successfully logged '{filename}' to processed list.")
    except IOError as e:
        logging.error(f"Failed to write '{filename}' to processed list file {file_path}: {e}")

# --- Date Calculation Functions ---

def get_target_date(current_date):
    """
    Determines the date embedded in the NDVI filename based on the current execution date.
    (Used for NDVI)
    """
    current_day = current_date.day
    if 1 <= current_day <= 10:
        target_date = datetime.date(current_date.year, current_date.month, 1) - datetime.timedelta(days=10)
    elif 11 <= current_day <= 20:
        target_date = datetime.date(current_date.year, current_date.month, 1)
    else:  # current_day >= 21
        target_date = datetime.date(current_date.year, current_date.month, 11)
    return target_date


def get_fapar_target_date(current_date):
    """
    Determines the date embedded in the FAPAR/FCOVER/LAI/DMP filename based on the current execution date.
    (Used for FAPAR, FCOVER, LAI, DMP)
    """
    current_day = current_date.day
    if 1 <= current_day <= 11:
        # 3rd dekad of previous month (the last day)
        first_of_month = datetime.date(current_date.year, current_date.month, 1)
        target_date = first_of_month - datetime.timedelta(days=1)
    elif 12 <= current_day <= 21:
        # 1st dekad (the 10th of the current month)
        target_date = datetime.date(current_date.year, current_date.month, 10)
    else:  # current_day >= 22
        # 2nd dekad (the 20th of the current month)
        target_date = datetime.date(current_date.year, current_date.month, 20)
    return target_date

# --- Central Execution Logic (Consolidating the three original functions) ---

def run_clipper_process(product_var, target_date, filename_base, base_dir_key, roi):
    """
    A unified function to execute the clipping process for a given product.
    Handles filename construction, processed list check, subprocess execution, and logging.
    """
    today = datetime.date.today()
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # 1. Base directory map and lookup
    base_dirs = {
        "NDVI": "/eodata/CLMS/bio-geophysical/vegetation_indices/ndvi_global_300m_10daily_v2",
        "DMP": "/eodata/CLMS/bio-geophysical/dry-gross_dry_matter_productivity/dmp_global_300m_10daily_v1",
        "FAPAR": "/eodata/CLMS/bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v1",
        "FCOVER": "/eodata/CLMS/bio-geophysical/vegetation_properties/fcover_global_300m_10daily_v1",
        "LAI": "/eodata/CLMS/bio-geophysical/vegetation_properties/lai_global_300m_10daily_v1",
    }

    base_dir = base_dirs.get(base_dir_key)
    if not base_dir:
        logging.error(f"Unknown product key: {base_dir_key}. Cannot determine base directory.")
        return

    # Path format: /base_dir/YEAR/MONTH/DAY/
    file_path_dir = os.path.join(base_dir, year, month, day)

    # Construct the full filename (used for check and argument)
    filename = filename_base.format(year=year, month=month, day=day, var=product_var)
    full_argument = os.path.join(file_path_dir, filename)

    log_prefix = f"[{product_var}-{roi} | Target Date: {target_date.strftime('%Y-%m-%d')}]"

    # --- CHECK if already processed ---
    processed_files = load_processed_list(PROCESSED_LIST_FILE)

    if filename in processed_files:
        logging.info(f"{log_prefix} SKIP: '{filename}' found in processed list. Subprocess skipped.")
        return  # Exit the function if already processed

    # Log execution details
    logging.info(f"{log_prefix} Starting processing. Execution Date: {today.strftime('%Y-%m-%d')}")
    logging.info(f"{log_prefix} Target Input File: {full_argument}")

    # 2. Define the command to execute
    command = [
        "python3",
        f"{product_var}/clip_clms_{product_var}_{roi}.py",
        full_argument
    ]

    logging.info(f"{log_prefix} Executing command: {' '.join(command)}")

    # 3. Execute the command using subprocess
    try:
        # check=True raises CalledProcessError on non-zero exit code
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        # print(command)
        # Log Success
        logging.info(f"{log_prefix} Script executed successfully.")
        # WRITE filename to processed list on SUCCESS
        write_to_processed_list(PROCESSED_LIST_FILE, filename)
        logging.debug(f"{log_prefix} STDOUT: {result.stdout.strip()}")

    except subprocess.CalledProcessError as e:
        # Log Error details
        logging.error(f"{log_prefix} Error executing script (Non-zero exit code {e.returncode}): {e}")
        logging.error(f"{log_prefix} STDERR:\n{e.stderr.strip()}")
        logging.warning(f"{log_prefix} Processed list was NOT updated due to script failure.")
    except FileNotFoundError:
        # Log File Not Found Error
        logging.error(f"{log_prefix} Error: Python or the script 'clip_clms_{product_var}_{roi}.py' was not found.")
        logging.warning(f"{log_prefix} Processed list was NOT updated due to execution error.")
    except Exception as e:
        # Log any other unexpected error
        logging.critical(f"{log_prefix} CRITICAL UNEXPECTED ERROR: {e}")
        logging.warning(f"{log_prefix} Processed list was NOT updated due to critical error.")


# --- Wrapper Functions (Simplified) ---

def run_ndvi_clipper(roi="AFRI"):
    """Wrapper for NDVI clipping process."""
    today = datetime.date.today()
    target_date = get_target_date(today)

    # NDVI filename structure
    filename_base = "c_gls_NDVI300_{year}{month}{day}0000_GLOBE_OLCI_V2.0.1_nc/c_gls_NDVI300_{year}{month}{day}0000_GLOBE_OLCI_V2.0.1.nc"

    run_clipper_process(
        product_var="NDVI",
        target_date=target_date,
        filename_base=filename_base,
        base_dir_key="NDVI",
        roi=roi
    )

def run_dmp_clipper(roi="AFRI"):
    """Wrapper for DMP clipping process."""
    today = datetime.date.today()
    target_date = get_fapar_target_date(today)

    # DMP filename structure
    filename_base = "c_gls_DMP300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1_nc/c_gls_DMP300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1.nc"

    run_clipper_process(
        product_var="DMP",
        target_date=target_date,
        filename_base=filename_base,
        base_dir_key="DMP",
        roi=roi
    )

def run_vegetation_properties_clipper(var="FAPAR", roi="AFRI"):
    """Wrapper for FAPAR, FCOVER, and LAI clipping processes."""
    today = datetime.date.today()
    target_date = get_fapar_target_date(today)

    # VP filename structure (uses {var} placeholder)
    filename_base = "c_gls_{var}300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1_nc/c_gls_{var}300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1.nc"

    run_clipper_process(
        product_var=var,
        target_date=target_date,
        filename_base=filename_base,
        base_dir_key=var, # var is FAPAR, FCOVER, or LAI
        roi=roi
    )

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("==================================================")
    logging.info("Clipper Automation Script Started")
    logging.info("==================================================")

    # Example runs
    run_ndvi_clipper(roi="AFRI")
    run_ndvi_clipper(roi="SOAM")
    run_vegetation_properties_clipper(var="FAPAR", roi="SOAM")
    run_vegetation_properties_clipper(var="FAPAR", roi="AFRI")
    run_vegetation_properties_clipper(var="FCOVER", roi="AFRI")
    run_vegetation_properties_clipper(var="FCOVER", roi="SOAM")
    run_vegetation_properties_clipper(var="LAI", roi="AFRI")
    run_vegetation_properties_clipper(var="LAI", roi="SOAM")
    run_dmp_clipper(roi="AFRI")
    run_dmp_clipper(roi="SOAM")

    logging.info("==================================================")
    logging.info("Clipper Automation Script Finished")
    logging.info("==================================================")

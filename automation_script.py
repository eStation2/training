import subprocess
import datetime
import os
import logging
from .DMP.clip_clms_DMP_AFRI import run_dmp_afri_clipping
from .DMP.clip_clms_DMP_SOAM import run_dmp_soam_clipping
from .NDVI.clip_clms_NDVI_AFRI import run_ndvi_afri_clipping
from .NDVI.clip_clms_NDVI_SOAM import run_ndvi_soam_clipping
from .FAPAR.clip_clms_FAPAR_AFRI import run_fapar_afri_clipping
from .FAPAR.clip_clms_FAPAR_SOAM import run_fapar_soam_clipping
from .FCOVER.clip_clms_FCOVER_AFRI import run_fcover_afri_clipping
from .FCOVER.clip_clms_FCOVER_SOAM import run_fcover_soam_clipping
from .LAI.clip_clms_LAI_AFRI import run_lai_afri_clipping
from .LAI.clip_clms_LAI_SOAM import run_lai_soam_clipping

# --- Configuration ---
PROCESSED_LIST_FILE = "/home/eouser/clms/config/processed_input_files.txt"
PROCESSED_OUTPUT_LIST = "/home/eouser/clms/config/processed_output_files.txt"
LOG_FILE = "/home/eouser/clms/logs/clipper_automation.log" # Common log file
# PROCESSED_LIST_FILE = "/data/processed_input_files.txt"
# LOG_FILE = "/data/clipper_automation.log" # Common log file
# --- Logging Setup ---
def setup_logging():
    """Configures the logging system to output to a file and the console."""
    logging.basicConfig(
        level=logging.DEBUG, # Set base level to INFO
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
    # --- NEW FILE EXISTENCE CHECK ---
    if not os.path.exists(full_argument):
        logging.error(f"{log_prefix} SKIP: Target Input File does NOT exist: '{full_argument}'")
        return  # Exit the function if the file doesn't exist
    # --------------------------------
    # --- CHECK if already processed ---
    processed_files = load_processed_list(PROCESSED_LIST_FILE)

    if filename in processed_files:
        logging.info(f"{log_prefix} SKIP: '{filename}' found in processed list. Subprocess skipped.")
        return  # Exit the function if already processed

    # Log execution details
    logging.info(f"{log_prefix} Starting processing. Execution Date: {today.strftime('%Y-%m-%d')}")
    logging.info(f"{log_prefix} Target Input File: {full_argument}")

    # Map product/ROI to its direct function call
    clipper_function = None
    if product_var == "DMP" and roi == "AFRI":
        clipper_function = run_dmp_afri_clipping
    elif product_var == "DMP" and roi == "SOAM":
        clipper_function = run_dmp_soam_clipping
    # Add other conditions here for other products (NDVI-AFRI, FAPAR-SOAM, etc.)
    elif product_var == "NDVI" and roi == "AFRI":
        clipper_function = run_ndvi_afri_clipping
    elif product_var == "NDVI" and roi == "SOAM":
        clipper_function = run_ndvi_soam_clipping()
    elif product_var == "FAPAR" and roi == "AFRI":
        clipper_function = run_fapar_afri_clipping()
    elif product_var == "FAPAR" and roi == "SOAM":
        clipper_function = run_fapar_soam_clipping()
    elif product_var == "FCOVER" and roi == "AFRI":
        clipper_function = run_fcover_afri_clipping()
    elif product_var == "FCOVER" and roi == "SOAM":
        clipper_function = run_fcover_soam_clipping()
    elif product_var == "LAI" and roi == "AFRI":
        clipper_function = run_lai_afri_clipping()
    elif product_var == "LAI" and roi == "SOAM":
        clipper_function = run_lai_soam_clipping()

    if not clipper_function:
        logging.error(f"{log_prefix} No direct function call available for this product/ROI. Skipping.")
        return

    logging.info(f"{log_prefix} Calling direct function: {clipper_function.__name__} for {full_argument}")

    # 3. Execute the function directly

    try:
        # 1. Execute the function
        # The function must be modified to return the zip file path (as in Step 1)
        zip_file_location = clipper_function(full_argument)

        # Check 1: Success based on function call completing without exception
        logging.info(f"{log_prefix} Script function executed successfully. Expected output: {zip_file_location}.")

        # 2. Check if the generated zip file exists at the reported location
        if os.path.exists(zip_file_location):
            # A. Track the original input file (filename) as processed
            write_to_processed_list(PROCESSED_LIST_FILE, filename)

            # B. Track the successful output zip file
            zip_filename_only = os.path.basename(zip_file_location)
            write_to_processed_list(PROCESSED_OUTPUT_LIST, zip_filename_only)

            logging.info(f"{log_prefix} SUCCESS: Output zip file found and lists updated: {zip_file_location}")

        else:
            # Check 2: Failure because the file wasn't created/found
            logging.error(
                f"{log_prefix} FAILURE: Function completed, but the expected zip file was NOT found at: {zip_file_location}")
            logging.warning(f"{log_prefix} Processed lists were NOT updated due to missing output file.")

    except Exception as e:
        # Log any error raised by the called clipping function
        logging.error(f"{log_prefix} Error executing function: {e}")
        logging.warning(f"{log_prefix} Processed list was NOT updated due to function failure.")

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

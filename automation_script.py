import subprocess
import datetime
import os
from calendar import monthrange

def get_target_date(current_date):
    """
    Determines the date embedded in the filename based on the current execution date.

    - If executed on the 1st-10th: Target is the 21st of the previous month.
    - If executed on the 11th-20th: Target is the 1st of the current month.
    - If executed on the 21st-last day: Target is the 11th of the current month.
    """
    current_day = current_date.day

    if 1 <= current_day <= 10:
        # Case 1: Execute 21st of previous month
        # Start at the 1st of the current month, then go back 10 days
        target_date = datetime.date(current_date.year, current_date.month, 1) - datetime.timedelta(days=10)
        # Now, target_date will naturally be the 21st of the previous month

    elif 11 <= current_day <= 20:
        # Case 2: Execute 1st of the current month
        target_date = datetime.date(current_date.year, current_date.month, 1)

    else:  # current_day >= 21
        # Case 3: Execute 11th of the current month
        target_date = datetime.date(current_date.year, current_date.month, 11)

    return target_date


def run_ndvi_clipper(roi="AFRI"):
    """
    Calculates the required input filename and executes the processing script.
    :param roi:
    """
    # Get the date this script is executing
    today = datetime.date.today()

    # Calculate the date corresponding to the required input file
    target_date = get_target_date(today)

    # Format the date parts for the path and filename
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # 1. Construct the full file path and argument
    base_dir = "/eodata/CLMS/bio-geophysical/vegetation_indices/ndvi_global_300m_10daily_v2"

    # Path format: /base_dir/YEAR/MONTH/DAY/
    file_path_dir = os.path.join(base_dir, year, month, day)

    # Filename format: c_gls_NDVI300_YYYYMMDD0000_GLOBE_OLCI_V2.0.1_nc
    filename = f"c_gls_NDVI300_{year}{month}{day}0000_GLOBE_OLCI_V2.0.1_nc/c_gls_NDVI300_{year}{month}{day}0000_GLOBE_OLCI_V2.0.1.nc"

    # Full argument to pass to the script
    full_argument = os.path.join(file_path_dir, filename)

    # 2. Define the command to execute
    command = [
        "python",
        f"NDVI/clip_clms_NDVI_{roi}.py",
        full_argument
    ]

    print(f"--- NDVI Clipper Automation ---")
    print(f"Execution Date: {today.strftime('%Y-%m-%d')}")
    print(f"Target Data Date: {target_date.strftime('%Y-%m-%d')}")
    print(f"Executing command: {' '.join(command)}")

    # 3. Execute the command using subprocess
    try:
        # Use subprocess.run to execute the command
        result = subprocess.run(command, check=True, capture_output=True, text=True)

        print("\nScript executed successfully.")
        # print(f"STDOUT:\n{result.stdout}") # Uncomment to see stdout

    except subprocess.CalledProcessError as e:
        print(f"\nError executing script: {e}")
        # print(f"STDERR:\n{e.stderr}") # Uncomment to see stderr
    except FileNotFoundError:
        print("\nError: Python or the script 'clip_clms_NDVI_AFRI.py' was not found.")

def run_dmp_clipper(roi="AFRI"):
    """
    Calculates the required input filename and executes the processing script.
    :param roi:
    """
    # Get the date this script is executing
    today = datetime.date.today()

    # Calculate the date corresponding to the required input file
    target_date = get_fapar_target_date(today)

    # Format the date parts for the path and filename
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # 1. Construct the full file path and argument
    base_dir = "/eodata/CLMS/bio-geophysical/dry-gross_dry_matter_productivity/dmp_global_300m_10daily_v1"

    # Path format: /base_dir/YEAR/MONTH/DAY/
    file_path_dir = os.path.join(base_dir, year, month, day)

    # Filename format: c_gls_NDVI300_YYYYMMDD0000_GLOBE_OLCI_V2.0.1_nc
    filename = f"c_gls_DMP300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1_nc/c_gls_DMP300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1.nc"

    # Full argument to pass to the script
    full_argument = os.path.join(file_path_dir, filename)

    # 2. Define the command to execute
    command = [
        "python",
        f"DMP/clip_clms_DMP_{roi}.py",
        full_argument
    ]

    print(f"--- DMP Clipper Automation ---")
    print(f"Execution Date: {today.strftime('%Y-%m-%d')}")
    print(f"Target Data Date: {target_date.strftime('%Y-%m-%d')}")
    print(f"Executing command: {' '.join(command)}")

    # 3. Execute the command using subprocess
    try:
        # Use subprocess.run to execute the command
        result = subprocess.run(command, check=True, capture_output=True, text=True)

        print("\nScript executed successfully.")
        # print(f"STDOUT:\n{result.stdout}") # Uncomment to see stdout

    except subprocess.CalledProcessError as e:
        print(f"\nError executing script: {e}")
        # print(f"STDERR:\n{e.stderr}") # Uncomment to see stderr
    except FileNotFoundError:
        print("\nError: Python or the script 'clip_clms_DMP.py' was not found.")


def get_fapar_target_date(current_date):
    """
    Determines the date embedded in the FAPAR filename based on the current execution date.

    Assuming the script is run one day after the decadal period ends:
    - If executed on the 1st-11th: Target is the 3rd dekad of the previous month (last day).
    - If executed on the 12th-21st: Target is the 10th of the current month.
    - If executed on the 22nd-last day: Target is the 20th of the current month.
    """
    current_day = current_date.day

    # --- Case 1: Execute 3rd dekad of previous month (the last day) ---
    if 1 <= current_day <= 11:
        # Get the first day of the current month
        first_of_month = datetime.date(current_date.year, current_date.month, 1)
        # Go back one day to find the last day of the previous month
        target_date = first_of_month - datetime.timedelta(days=1)

    # --- Case 2: Execute 1st dekad (the 10th of the current month) ---
    elif 12 <= current_day <= 21:
        target_date = datetime.date(current_date.year, current_date.month, 10)

    # --- Case 3: Execute 2nd dekad (the 20th of the current month) ---
    else:  # current_day >= 22
        target_date = datetime.date(current_date.year, current_date.month, 20)

    return target_date


def run_vegetation_properties_clipper(var="FAPAR", roi="AFRI"):
    """
    Calculates the required input filename and executes the FAPAR processing script.
    :param roi:
    :param var:
    """
    # Get the date this script is executing
    today = datetime.date.today()

    # Calculate the date corresponding to the required input file
    target_date = get_fapar_target_date(today)

    # Format the date parts for the path and filename
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    # 1. Construct the full file path and argument
    # Note: Updated base path for FAPAR product
    if var == "FAPAR": base_dir = "/eodata/CLMS/bio-geophysical/vegetation_properties/fapar_global_300m_10daily_v1"
    if var == "FCOVER": base_dir = "/eodata/CLMS/bio-geophysical/vegetation_properties/fcover_global_300m_10daily_v1"
    if var == "LAI": base_dir = "/eodata/CLMS/bio-geophysical/vegetation_properties/lai_global_300m_10daily_v1"
    # Path format: /base_dir/YEAR/MONTH/DAY/
    file_path_dir = os.path.join(base_dir, year, month, day)

    # Filename format: c_gls_FAPAR300-RT0_YYYYMMDD0000_GLOBE_OLCI_V1.1.1_nc
    filename = f"c_gls_{var}300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1_nc/c_gls_{var}300-RT0_{year}{month}{day}0000_GLOBE_OLCI_V1.1.1.nc"

    # Full argument to pass to the script
    full_argument = os.path.join(file_path_dir, filename)

    # 2. Define the command to execute
    command = [
        "python",
        f"{var}/clip_clms_{var}_{roi}.py",  # Target script
        full_argument
    ]

    print(f"--- vegetation properties Clipper Automation ---")
    print(f"Execution Date: {today.strftime('%Y-%m-%d')}")
    print(f"Target Data Date: {target_date.strftime('%Y-%m-%d')}")
    print(f"Executing command: {' '.join(command)}")

    # 3. Execute the command using subprocess
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        print("\nScript executed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"\nError executing script: {e}")
        # print(f"STDERR:\n{e.stderr}")
    except FileNotFoundError:
        print("\nError: Python or the script 'clip_clms_FAPAR_AFRI.py' was not found.")

if __name__ == "__main__":
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
# Mins  Hours  Day_of_Month  Month  Day_of_Week  Command_to_execute
#   0     0     1,11,21       * * /usr/bin/python3 /path/to/your/automation_script.py
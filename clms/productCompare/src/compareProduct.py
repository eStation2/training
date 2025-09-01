import os
import tempfile
import zipfile
from datetime import datetime
import pytz
import socket
import sys
import pkg_resources

from compareNetCDF import compare_netcdf
from compareQL import compare_geotiff
from compareXml import compare_xml
from compareZip import compare_zip

USED_MODULES = ['GDAL', 'numpy', 'xarray', 'rasterio', 'zipp']

def get_product_files_dict(unzip_folder, zip_contents):
    product_dict = {}
    for content in zip_contents:
        if '.nc' in content:
            product_dict['NetCDF'] = os.path.join(unzip_folder, content)
        if '.tiff' in content:
            product_dict['QL'] = os.path.join(unzip_folder, content)
        if '.xml' in content:
            product_dict['xml'] = os.path.join(unzip_folder, content)
    
    return product_dict

if __name__ == "__main__":
    import argparse
    import json
    from pprint import pprint
    
    parser = argparse.ArgumentParser(description="Compare two product zip files")
    parser.add_argument("ref_product", type=str, help="Reference product zip file")
    parser.add_argument("new_product", type=str, help="New product zip file")
    parser.add_argument("-t", "--tempFolder", type=str, required=False, default=None, help="Temperary folder, if omitted, systems default will be used")
    parser.add_argument("-r", "--reportFile", type=str, required=False, default=None, help="Report file - json file containing differences")
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress terminal output')
    
    args = parser.parse_args()
    

    tmpFolder = None
    try:
        tmpFolder = tempfile.TemporaryDirectory(dir=args.tempFolder)
        print(f" > Temporary folder: {tmpFolder.name}")
        args.tempFolder = tmpFolder.name

        print(" > Extracting the products")
        refFolder = os.path.join(args.tempFolder, 'ref')
        newFolder = os.path.join(args.tempFolder, 'new')
        
        with zipfile.ZipFile(args.ref_product, 'r') as zf:
            zf.extractall(refFolder)
            ref_contents = get_product_files_dict(refFolder, zf.namelist())
        
        with zipfile.ZipFile(args.new_product, 'r') as zf:
            zf.extractall(newFolder)
            new_contents = get_product_files_dict(newFolder, zf.namelist())
        
        versionDict = {'Python': sys.version}
        for dist in pkg_resources.working_set:
            if dist.project_name in USED_MODULES:
                versionDict[dist.project_name] = dist.version
    
        report = {
            'environment': {
                'host': socket.gethostname(),
                'time_stamp': datetime.now(pytz.timezone("Europe/Brussels")).strftime("%Y-%m-%d %H:%M:%S %Z%z"),
                'versions': versionDict
                }
            }

        print(" > Comparing zip")
        report['zip'] = compare_zip(args.ref_product, args.new_product)

        print(" > Comparing NetCDF")
        report['NetCDF'] = compare_netcdf(ref_contents['NetCDF'], new_contents['NetCDF'])

        #print(" > Comparing Quick Look")
        #report['QL'] = compare_geotiff(ref_contents['QL'], new_contents['QL'])

        print(" > Comparing Product Description")
        report['xml'] = compare_xml(ref_contents['xml'], new_contents['xml'])

        if not args.quiet:
            pprint(report, sort_dicts=False)

        if args.reportFile:
            print(f"Saving report to {args.reportFile}")
            with open(args.reportFile, 'w') as fp:
                fp.write(json.dumps(report, indent=2))
    except Exception as e:
        raise e
    finally:
        if tmpFolder:
            tmpFolder.cleanup()
from copy import deepcopy

import numpy as np
import xarray as xr
from common import compare_file_info

def compare_global_attributes(file1, file2):
    with xr.open_dataset(file1) as ds1, xr.open_dataset(file2) as ds2:
        attrs1 = ds1.attrs
        attrs2 = ds2.attrs
    
    diffs = {key: (attrs1.get(key), attrs2.get(key)) for key in set(attrs1) | set(attrs2) if attrs1.get(key) != attrs2.get(key)}
    
    return diffs

def compare_band_attributes(file1, file2):
    with xr.open_dataset(file1) as ds1, xr.open_dataset(file2) as ds2:
        band_diffs = {}

        for var in set(ds1.data_vars) | set(ds2.data_vars):
            if var in ds1 and var in ds2:
                attrs1 = ds1[var].attrs
                attrs2 = ds2[var].attrs
                attr_diffs = {}
                for attr in attrs1.keys() | attrs2.keys():
                    if type(attrs1.get(attr)) == np.ndarray:
                        if (attrs1.get(attr) != attrs2.get(attr)).any():
                            attr_diffs[attr] = (attrs1.get(attr), attrs2.get(attr))
                    elif attrs1.get(attr) != attrs2.get(attr):
                        attr_diffs[attr] = (attrs1.get(attr), attrs2.get(attr))
                
                if attr_diffs: # if there is a difference
                    band_diffs[var] = deepcopy(attr_diffs)
            elif var not in ds1:
                band_diffs[var] = {f"{var} missing in {file1}"}
            else:
                band_diffs[var] = {f"{var} missing in {file2}"}
    
    return band_diffs

def compare_band_encoding(file1, file2):
    with xr.open_dataset(file1) as ds1, xr.open_dataset(file2) as ds2:
        encoding_diffs = {}

        for var in set(ds1.data_vars) | set(ds2.data_vars):
            if var in ds1 and var in ds2:
                enc1 = ds1[var].encoding
                del enc1['source'] # source will always be different
                enc2 = ds2[var].encoding
                del enc2['source'] # source will always be different
                if enc1 != enc2:
                    encoding_diffs[var] = (str(enc1), str(enc2))
            elif var not in ds1:
                encoding_diffs[var] = {f"{var} missing in {file1}"}
            else:
                encoding_diffs[var] = {f"{var} missing in {file2}"}
    
    return encoding_diffs

def compare_pixel_values(file1, file2):
    with xr.open_dataset(file1) as ds1, xr.open_dataset(file2) as ds2:
        var_list = []
        for var in set(ds1.data_vars) | set(ds2.data_vars):
            if var not in ds1:
                diffs[var] = {f"{var} missing in {file1}"}
            if var not in ds2:
                diffs[var] = {f"{var} missing in {file2}"}
            else:
                var_list.append(var)
        
    diffs = {}
    for var in var_list:
        diffs[var] = compare_pixel_values_per_variable(file1, file2, var)

    return diffs

def compare_pixel_values_per_variable(file1, file2, variable):
    # Open datasets with dask for efficient processing
    ds1 = xr.open_dataset(file1, chunks={}, decode_cf=False)
    ds2 = xr.open_dataset(file2, chunks={}, decode_cf=False)
    
    var1 = ds1[variable]
    var2 = ds2[variable]
    
    t_offset = 0
    x_offset = 0
    y_offset = 0
    
    diffs = {
        "count": 0,      # Number of differing pixels
        "locations": [], # Pixel locations (index-based) will be limited to 100 per chunk
        "values": []     # The values, will be limited to 100 per chunck
            }
    if  var1.chunks:
        t_chunks = var1.chunks[0]
        y_chunks = var1.chunks[1]
        x_chunks = var1.chunks[2]
        print(f"Checking {variable} ({len(t_chunks)*len(y_chunks)*len(x_chunks)} chunks): ", end="")
        # Loop over chunks using Dask's blockwise access
        for t in t_chunks:  # loop over time chunks
            t_end = t_offset + t
            for i in y_chunks: # loop over lat chunks
                y_end = y_offset + i
                for j in x_chunks:  # loop over lon chunks
                    print(".", end="")
                    x_end = x_offset + j
                    chunk1 = var1.isel(
                        time=slice(t_offset, t_end), 
                        lat=slice(y_offset, y_end),
                        lon=slice(x_offset, x_end))
                    chunk2 = var2.isel(
                        time=slice(t_offset, t_end), 
                        lat=slice(y_offset, y_end),
                        lon=slice(x_offset, x_end))
                    data1 = chunk1.compute()
                    data2 = chunk2.compute()
                    diff_mask = (data1.values != data2.values)
                    differences = np.where(diff_mask)
                    
                    count = len(differences[0])
                    diffs['count'] += count
                    if count: # if there are difference
                        locations = list(zip(*differences))[:min(count, 100)]
                        locations = [(lambda t, y, x : (int(t + t_offset), int(y + y_offset), int(x + x_offset)))(z, y, x) for (z, y, x) in locations]
                        values = [(float(data1.values[idx]), float(data2.values[idx])) for idx in list(zip(*differences))[:min(count, 100)]]
                        diffs['locations'].append(locations)
                        diffs['values'].append(values)                                 
                    x_offset = x_end
                y_offset = y_end
            t_offset = t_end
            print(" done")
        diffs['percent'] = diffs['count'] * 100.0 / (var1.shape[0] * var1.shape[1] * var1.shape[2])  
    else:
        data1 = var1.compute()
        data2 = var2.compute()
        diff_mask = (data1.values != data2.values)
        differences = np.where(diff_mask)
        
        count = len(differences[0])
        diffs['count'] += count
        if count: # if there are difference
            locations = list(zip(*differences))[:min(count, 100)]
            values = [(data1.values[idx], data2.values[idx]) for idx in list(zip(*differences))[:min(count, 100)]]
            diffs['locations'].append(locations)
            diffs['values'].append(values)         
    
    ds1.close()
    ds2.close()
    
    return diffs

def compare_nc_transform(file1, file2):
    with xr.open_dataset(file1) as ds1:
        ul1_lat = ds1['lat'].values[0]
        ul1_lon = ds1['lon'].values[0]
        lat1_px_size = ds1['lat'].values[1] - ds1['lat'].values[0]
        lon1_px_size = ds1['lon'].values[1] - ds1['lon'].values[0]
        transform1 = (lon1_px_size, 0.0, ul1_lon, 0.0, lat1_px_size, ul1_lat)
    with xr.open_dataset(file2) as ds2:
        ul2_lat = ds2['lat'].values[0]
        ul2_lon = ds2['lon'].values[0]
        lat2_px_size = ds2['lat'].values[1] - ds2['lat'].values[0]
        lon2_px_size = ds2['lon'].values[1] - ds2['lon'].values[0]
        transform2 = (lon2_px_size, 0.0, ul2_lon, 0.0, lat2_px_size, ul2_lat)
    transform_diff = ()
    if transform1 != transform2:
        transform_diff = (transform1, transform2)

    return transform_diff

def compare_dimensions(file1, file2):
    with xr.open_dataset(file1) as ds1, xr.open_dataset(file2) as ds2:
        
        dimension_diffs = {}
        
        for coord in ds1.coords:
            if not ds1[coord].equals(ds2[coord]):  # Checks if coordinate values are different
                dimension_diffs_list = list(zip(list(ds1[coord].values), list(ds2[coord].values)))
                nr_diffs = len(dimension_diffs_list)
                dimension_diffs[coord] = {
                    "count": nr_diffs,
                    "first values": dimension_diffs_list[:min(10,nr_diffs)],
                    "last values": dimension_diffs_list[max(-10,-nr_diffs):]
                    }
    
    return dimension_diffs

def compare_netcdf(file1, file2):
    report = {}
    
    print("File info")
    report['file_info'] = compare_file_info(file1, file2)
    
    print("Global attributes")
    report['global_attr_diffs'] = compare_global_attributes(file1, file2)
    if 'history' not in report['global_attr_diffs']:
        report['global_attr_diffs']['history'] = "Warning - Both 'history' attributes are identical!"
            
    print("Band attributes")
    report['band_attr_diffs'] = compare_band_attributes(file1, file2)
    
    print("Transform matrix")
    report['transform_diffs'] = compare_nc_transform(file1, file2)

    print("Dimensions")
    report['dimensions'] = compare_dimensions(file1, file2)
    
    print("Band encoding")
    report['band_encoding_diff'] = compare_band_encoding(file1, file2)

    print("Pixel level differences")
    report['pixel_diffs'] = compare_pixel_values(file1, file2)

    return report
  
if __name__ == "__main__":
    import argparse
    import json
    from pprint import pprint
    
    parser = argparse.ArgumentParser(description="Compare two NetCDF files")
    parser.add_argument("ref_netcdf", type=str, help="Reference NetCDF file")
    parser.add_argument("new_netcdf", type=str, help="New NetCDF file")
    parser.add_argument("-t", "--tempFolder", type=str, required=False, default=None, help="Temperary folder, if omitted, systems default will be used")
    parser.add_argument("-r", "--reportFile", type=str, required=False, default=None, help="Report file - json file containing differences")
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress terminal output')
    
    args = parser.parse_args()  
    report = compare_netcdf(args.ref_netcdf, args.new_netcdf)

    if not args.quiet:
        pprint(report, sort_dicts=False)

    if args.reportFile:
        print(f"Saving report to {args.reportFile}")
        with open(args.reportFile, 'w') as fp:
            fp.write(json.dumps(report, indent=2))

import numpy as np
import rasterio
from common import compare_file_info

# Open the first GeoTIFF

def compare_meta(file1, file2):
    with rasterio.open(file1) as src1:
        metadata1 = src1.meta
        tags1 = src1.tags()
        band_tags1 = [src1.tags(i) for i in src1.indexes]

    with rasterio.open(file2) as src2:
        metadata2 = src2.meta
        tags2 = src2.tags()
        band_tags2 = [src2.tags(i) for i in src2.indexes]
    
    diffs = {}
    diffs['file_meta_diffs'] = {key: (metadata1.get(key), metadata2.get(key)) for key in metadata1 | metadata2 if metadata1.get(key) != metadata2.get(key)}
    diffs['dataset_meta_diffs'] = {key: (tags1.get(key), tags2.get(key)) for key in tags1 | tags2 if tags1.get(key) != tags2.get(key)}
    diffs['band_meta_diffs'] = {}
    for i in range(len(band_tags1)):
        diffs['band_meta_diffs'][f'band_{i+1}'] = {key: (band_tags1[i].get(key), band_tags2[i].get(key)) for key in band_tags1[i] | band_tags2[i] if band_tags1[i].get(key) != band_tags2[i].get(key)}

    return diffs

def compare_dataset(file1, file2):
    with rasterio.open(file1) as src1:
        size1 = (src1.width, src1.height)
        bounds1 = src1.bounds
        colormap1 = src1.colormap(1)

    with rasterio.open(file2) as src2:
        size2 = (src2.width, src2.height)
        bounds2 = src2.bounds
        colormap2 = src2.colormap(1)
    
    diff = {}
    if size1 != size2:
        diff['size'] = (size1, size2)
    if bounds1 != bounds2:
        diff['bounds'] = (bounds1, bounds2)
    if colormap1 != colormap2:
        diff['colormap'] = (colormap1, colormap2)

    return diff

def compare_pixel_values(file1, file2):
    # Open the first raster
    with rasterio.open(file1) as src1:
        data1 = src1.read(1) 

    with rasterio.open(file2) as src2:
        data2 = src2.read(1)  # Read the first band

    # Ensure both rasters have the same shape
    if data1.shape != data2.shape:
        return "Raster dimensions do not match!"

    # Compute pixel-level differences
    diff_mask = data1 != data2
    
    # Find differing pixel locations
    differences = np.where(diff_mask)
    
    # Gather pixel values from both datasets at differing locations
    # take only the first 10 into account
    diffs = {
        "count": len(differences[0]),  # Number of differing pixels
        "locations": list(zip(*differences))[:min(10,len(differences[0]))],  # Pixel locations (index-based)
        "values": [(data1[idx], data2[idx]) for idx in zip(*differences)][:min(10,len(differences[0]))]
    }
    # Cast the type of the locations to an int, otherwise a json serialize will break on the numpy datatypes
    diffs["locations"] = list(map(lambda t: (int(t[0]), int(t[1])), diffs["locations"]))
    diffs["values"] = list(map(lambda t: (int(t[0]), int(t[1])), diffs["values"]))
    
    return diffs

def compare_geotiff(file1, file2):
    report = {}
    print("File info")
    report['file_info'] = compare_file_info(file1, file2)
    
    print("Metadata")
    report.update(compare_meta(file1, file2))

    print("Dataset")
    report['dataset_diffs'] = compare_dataset(file1, file2)

    print("Pixel level differences")
    report['pixel_diffs'] = compare_pixel_values(file1, file2)

    return report
  
if __name__ == "__main__":
    import argparse
    import json
    from pprint import pprint
    
    parser = argparse.ArgumentParser(description="Compare two GeoTiff files")
    parser.add_argument("ref_geotiff", type=str, help="Reference GeoTiff file")
    parser.add_argument("new_geotiff", type=str, help="New GeoTiff file")
    parser.add_argument("-r", "--reportFile", type=str, required=False, default=None, help="Report file - json file containing differences")
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress terminal output')
    
    args = parser.parse_args()  
    report = compare_geotiff(args.ref_geotiff, args.new_geotiff)

    if not args.quiet:
        pprint(report, sort_dicts=False)

    if args.reportFile:
        print(f"Saving report to {args.reportFile}")
        with open(args.reportFile, 'w') as fp:
            fp.write(json.dumps(report, indent=2))

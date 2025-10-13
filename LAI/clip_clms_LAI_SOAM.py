import netCDF4 as nc
import zipfile
import re, shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange

import os, sys
import numpy as np
from osgeo import gdal
import glob
def createColorTable(colorTable):
    with open(colorTable, 'r') as hColorTable:
        ctLines = hColorTable.readlines()
        
    ct = gdal.ColorTable()
    for lineId, line in enumerate(ctLines):
        line_ = line.replace('   ',' ').replace('  ',' ').replace('\t',' ').strip().strip('<').strip('>').strip('Entry ').strip('/').split(' ')
        ct.SetColorEntry(lineId,(int(line_[0].split('=')[1].strip('"')), int(line_[1].split('=')[1].strip('"')), int(line_[2].split('=')[1].strip('"')), int(line_[3].split('=')[1].strip('"'))))
        
    return ct

def createQuicklook(qlDict):
    '''
    createQuicklook
    
        creates quicklook from input netCDF using gdal API
        
        the outFilename is always, appended with .tiff' to generate a quicklook.tiff
        optionally, a png file can be created
        e.g. cgl_DMP-RT0_QL_201808100000_X18Y03_S3_V1.1.rc1.png
        
        the output type of a quicklook is ALWAYS byte
        
        src_Type, src_min, src_max are by default derived from the input dataset
    
    Params:
    ------
    qlDict: dict
        configuration dictionary holding following settings:
        mandatory:
            inFilename: str
                path / name of input netCDF file
            outFilename: str
                path / basename of output file - without filename extension
            colorTable: str
                path / name of the colorTable (same file content as used to append to vrt file)
            ql_BandName: str
                the bandName to extract
            ql_Subsample: list of int
                the subsampling to be used e.g. [5,5] for 333m, [25,25] for 1km
            ql_NDV: int 0...255
                the no_data_value to be used for the ql image
            ql_Min, ql_Max: int 0..255
                the valid_range (minimum, maximum) data value for the ql image 
                
        optional:
            ql_Additional_Format: str
                # QL.tif will be generated anyhow
                # additionally 'PNG' can be created created
                # for now, no other additional types supported
            src_Min, src_Max: int
                defining the valid_range of the input_band, default taken from 'valid_range' attribute
                used to overrule input netCDF settings
    
    Returns:
    -------
    err, errMsg: err=0 if OK
    '''
    
    try:
        # verify input parameter values
        if (not qlDict['ql_NDV'] in range(0,256)) or \
           (not qlDict['ql_Min'] in range(0,256)) or \
           (not qlDict['ql_Max'] in range(0,256)):
            return 1, 'Error createQuicklook, ql_ parameters not within the expected output gdal.GDT_Byte range '

        if not type(qlDict['ql_Subsample']) == list:
            return 1, 'Error createQuicklook, ql_Subsample should be of type list'
            
        if not os.path.exists(qlDict['inFilename']):
            return 1, 'Error createQuicklook, {} not existing'.format(qlDict['inFilename'])

        # get input data src_Type, src_Min, src_Max, src_NDV
        src_ds = gdal.Open(qlDict['inFilename'], gdal.GA_ReadOnly)

        if src_ds is None:
            return 1, 'Error createQuicklook. Open failed {}'.format(qlDict['inFilename'])

        try:
            subdataset = 'NETCDF:"' + qlDict['inFilename'] + '":' + qlDict['ql_BandName']
            
            src_ds_sd = gdal.Open(subdataset, gdal.GA_ReadOnly)
            
            src_NDV  = src_ds_sd.GetRasterBand(1).GetNoDataValue()
            src_Type = src_ds_sd.GetRasterBand(1).DataType
    
            src_bandMeta = src_ds_sd.GetRasterBand(1).GetMetadata()
            if 'valid_range' in src_bandMeta:
                validRange = src_bandMeta['valid_range'].strip('{').strip('}').split(',')
                validRange = list(map(np.int16, validRange))   #type only needed to compare with ql_Min/ql_Max, not for metedata-type
                [src_Min, src_Max] = validRange
            else:
                src_Min = None
                src_Max = None
                
            #close the subdataset and the whole dataset
            src_ds_sd = None
            src_ds    = None
        except:
            return 1, 'Error createQuicklook. Error reading bands from {}'.format(qlDict['inFilename'])
            
        #create output
        ql_Type         = gdal.GDT_Byte
        #GIOG-1358: PROFILE=GeoTIFF removes scale and offset TAGS from GeoTIFF's
        creationOptions = ['COMPRESS=LZW', 'PROFILE=GeoTIFF'] 
        openOptions     = ['HONOUR_VALID_RANGE=FALSE']
        #GIOG-1601: DMP quicklook needs flag values to be set to no_data
        if 'gdal_open_option' in qlDict:
            openOptions = qlDict['gdal_open_option']
        
        # outputDir, _ = os.path.split(qlDict['outFilename'])
        # os.makedirs(outputDir, exist_ok=True)
        
        qlTifFilename  = qlDict['outFilename']+'.tiff'
        qlPngFilename  = qlDict['outFilename']+'.png'
    
        if 'src_Min' in qlDict and type(qlDict['src_Min']) == int:
            print('createQuicklook: overruling input src_Min {} by cfg {}'.format(src_Min, qlDict['src_Min']))
            src_Min = qlDict['src_Min']
        if 'src_Max' in qlDict and type(qlDict['src_Max']) == int:
            print('createQuicklook: overruling input src_Max {} by cfg {}'.format(src_Max, qlDict['src_Max']))
            src_Max = qlDict['src_Max']

        scaleParams = None
        if not (src_Min == qlDict['ql_Min']) or not (src_Max == qlDict['ql_Max']):
        #if not (src_Type == ql_Type):
            if src_Min == None and src_Max == None:
                return 1, 'Error createQuicklook. impossible to convert inputband_Type to Byte without valid_range. Provide "src_Min" and "src_Max in qlDict'
            scaleParams = [[src_Min,src_Max,qlDict['ql_Min'],qlDict['ql_Max']]] #list of band-list to be provided
    
        #translate subdataset to GTIFF and scale immediate to QL output format
        translateOptions = gdal.TranslateOptions(format='GTIFF', outputType=ql_Type, bandList=[1], 
                                                 widthPct=qlDict['ql_Subsample'][0], heightPct=qlDict['ql_Subsample'][0], 
                                                 creationOptions=creationOptions, scaleParams=scaleParams, noData=qlDict['ql_NDV'])
        #GIOG-1358: Do not interprete the NetCDF Valid range, otherwise all flags will be no_data
        src_ds_sd = gdal.OpenEx(subdataset, gdal.GA_ReadOnly, open_options=openOptions)
        gdal.Translate(qlTifFilename, src_ds_sd, options=translateOptions)
        src_ds_sd = None
        
        # remove metadata
        # ERROR 6: The PNG driver does not support update access to existing datasets.
        dst_ds = gdal.Open(qlTifFilename, gdal.GA_Update)
        dst_ds.SetMetadata({})
        dst_ds.GetRasterBand(1).SetMetadata({})
        
        # add colorTable
        if os.path.exists(qlDict['colorTable']):
            ct = createColorTable(qlDict['colorTable'])
            dst_ds.GetRasterBand(1).SetColorTable(ct)
        dst_ds.FlushCache()
        dst_ds = None
        
        if 'ql_Additional_Format' in qlDict:
            if not qlDict['ql_Additional_Format'] == 'PNG':
                return 1, 'Error createQuicklook. ql_Additional_Format only supports PNG '
                
            #translate to final PNG
            translateOptions = gdal.TranslateOptions(format='PNG', bandList=[1])
            gdal.Translate(qlPngFilename, qlTifFilename, options=translateOptions)
        
        return True
            
    except:
        errMess = str(sys.exc_info()[1])
        errLine = sys.exc_info()[2].tb_lineno
        print('Error createQuicklook at {} \n{}'.format(errLine, errMess))

def replace_xml_parameters(xml_file_path, params_dict, output_file_path=None):
    """
    Replaces parameters within an XML file, identified by '$' prefix, with provided values.

    Args:
        xml_file_path (str): Path to the XML file.
        params_dict (dict): Dictionary containing parameter names (without '$') as keys
                           and their corresponding replacement values as values.
        output_file_path (str, optional): Path to save the modified XML. If None,
                           the original file is overwritten. Defaults to None.

    Returns:
        str:  The modified XML content as a string.  Returns an empty string
              if the file does not exist or an error occurs during processing.
              Returns None if the xml file is empty.
    """
    try:
        import lxml.etree as ET
        # 1. Read the XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except FileNotFoundError:
        print(f"Error: File not found at {xml_file_path}")
        return ""
    except ET.ParseError as e:
        print(f"Error: XML parsing error in {xml_file_path}: {e}")
        return ""
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return ""

    # Check if the XML file is empty
    if not root:
        return None

    def replace_text(element):
        """
        Helper function to recursively replace text within an XML element
        and its children.

        Args:
            element (ET.Element): The XML element to process.
        """
        if element.text:
            for key, value in params_dict.items():
                # Use a regular expression to find the parameter with '$'
                pattern = r'\$' + re.escape(key)
                element.text = re.sub(pattern, str(value), element.text)
        for child in element:
            replace_text(child)  # Recurse into child elements
        # After processing children, process attributes of the current element.
        for attr_key, attr_value in element.attrib.items():
            for key, value in params_dict.items():
                pattern = r'\$' + re.escape(key)
                element.attrib[attr_key] = re.sub(pattern, str(value), attr_value)

    # 2. Iterate through the dictionary and replace the placeholders
    replace_text(root)

    # 3. Save the modified XML to a file or return as a string
    if output_file_path:
        try:
            tree.write(output_file_path, encoding="utf-8", xml_declaration=True)
            print(f"Modified XML saved to {output_file_path}")
        except Exception as e:
            print(f"Error saving to {output_file_path}: {e}")
            return ""
    else:
        # Return the modified XML as a string
        return ET.tostring(root, encoding="utf-8").decode()



def clip_all_vars_netcdf4(input_nc_file, output_nc_file, lat_range=None, lon_range=None,
                           origin_lat=None, origin_lon=None, clip_width=None, clip_height=None,
                           compress=True, complevel=9, data_vars = None, identifier=None, parent_identifier=None):
    """
    Clips all variables in a NetCDF file based on either latitude/longitude ranges
    OR by providing an origin (top-left) coordinate and desired width/height in pixels.
    Specifically adapted for the structure of c_gls_LAI300 data.
    Adds optional zlib compression.
    :param identifier:
    :param parent_identifier:
    """
    try:
        with nc.Dataset(input_nc_file, 'r') as src, nc.Dataset(output_nc_file, 'w', format='NETCDF4') as dst:
            # Disable automatic masking for the source dataset
            # This is the key change to avoid interpreting valid_range and _FillValue during read.
            src.set_auto_mask(False)
            # Copy global attributes
            dst.setncatts(src.__dict__)
            dst.identifier = identifier
            dst.parent_identifier = parent_identifier
            dst.title = src.title.replace("GLOBE", "SOAM")
            dst.history = src.history+ " \n"+datetime.today().strftime("%Y-%m-%d")+': CLMS Clipping tool for South-America' #+src.history.split(':')[-1]
            # Get latitude and longitude variables and values
            lat_var = src.variables['lat']
            lon_var = src.variables['lon']
            lat_values = lat_var[:]
            lon_values = lon_var[:]

            lat_indices = slice(None)
            lon_indices = slice(None)
            output_height = len(src.dimensions['lat'])
            output_width = len(src.dimensions['lon'])

            # --- Determine clipping indices based on origin_lat/lon and width/height ---
            if origin_lat is not None and origin_lon is not None and clip_width is not None and clip_height is not None:
                # Find the index of the origin latitude (top-left)
                # Assuming 'lat' values are typically decreasing (from North to South)
                # Find the closest latitude value to origin_lat (top-left)
                lat_start_idx = np.argmin(np.abs(lat_values - origin_lat))

                # Find the closest longitude value to origin_lon (top-left)
                lon_start_idx = np.argmin(np.abs(lon_values - origin_lon))

                # --- MODIFICATION START ---
                # Remove one pixel from the left (increase lon_start_idx by 1)
                lon_start_idx = min(lon_start_idx + 1, len(lon_values) - 1) # Ensure it doesn't go out of bounds

                # Add one pixel to the right (increase clip_width by 1)
                adjusted_clip_width = clip_width #+ 1
                # --- MODIFICATION END ---

                # Calculate the end indices based on origin and desired width/height
                lat_end_idx = lat_start_idx + clip_height
                lon_end_idx = lon_start_idx + adjusted_clip_width # Use adjusted_clip_width here

                # Ensure indices are within bounds of the original data
                lat_end_idx = min(lat_end_idx, len(lat_values))
                lon_end_idx = min(lon_end_idx, len(lon_values))

                # Adjust start index if the calculated end index exceeds the bounds
                # This ensures we get exactly `clip_height` or `adjusted_clip_width` pixels
                if (lat_end_idx - lat_start_idx) < clip_height:
                    lat_start_idx = max(0, lat_end_idx - clip_height)
                if (lon_end_idx - lon_start_idx) < adjusted_clip_width: # Use adjusted_clip_width here
                    lon_start_idx = max(0, lon_end_idx - adjusted_clip_width)


                lat_indices = slice(lat_start_idx, lat_end_idx)
                lon_indices = slice(lon_start_idx, lon_end_idx)

                output_height = len(lat_values[lat_indices])
                output_width = len(lon_values[lon_indices])

                if output_height != clip_height or output_width != adjusted_clip_width: # Compare with adjusted_clip_width
                    print(f"Warning: Clipped dimensions ({output_width}x{output_height}) do not exactly match "
                          f"requested dimensions ({adjusted_clip_width}x{clip_height}). This can happen if the origin "
                          f"is too close to the edge of the input data.")

            # --- OR Determine clipping indices based on lat_range/lon_range (original logic) ---
            elif lat_range is not None or lon_range is not None:
                if lat_range is not None:
                    lat_start_index = np.argmin(np.abs(lat_values - lat_range[0]))
                    lat_end_index = np.argmin(np.abs(lat_values - lat_range[1])) + 1
                    lat_indices = slice(min(lat_start_index, lat_end_index), max(lat_start_index, lat_end_index))
                    output_height = len(lat_values[lat_indices])

                if lon_range is not None:
                    lon_start_index = np.argmin(np.abs(lon_values - lon_range[0]))
                    lon_end_index = np.argmin(np.abs(lon_values - lon_range[1])) + 1

                    # --- MODIFICATION FOR lon_range START ---
                    # Remove one pixel from the left
                    lon_start_index = min(lon_start_index + 1, len(lon_values) - 1)
                    # Add one pixel to the right (by extending the end index)
                    lon_end_index = min(lon_end_index + 1, len(lon_values))
                    # --- MODIFICATION FOR lon_range END ---

                    lon_indices = slice(min(lon_start_index, lon_end_index), max(lon_start_index, lon_end_index))
                    output_width = len(lon_values[lon_indices])
            else:
                print("No clipping parameters provided. Output file will be a full copy.")

            # --- Create Dimensions and Variables in Output NetCDF ---
            dst.createDimension('lat', output_height)
            out_lat_var = dst.createVariable('lat', lat_var.dtype, ('lat',), zlib=compress, complevel=complevel)
            out_lat_var[:] = lat_values[lat_indices]
            out_lat_var.setncatts(lat_var.__dict__)

            dst.createDimension('lon', output_width)
            out_lon_var = dst.createVariable('lon', lon_var.dtype, ('lon',), zlib=compress, complevel=complevel)
            out_lon_var[:] = lon_values[lon_indices]
            out_lon_var.setncatts(lon_var.__dict__)

            # Copy the time dimension and variable with compression
            if 'time' in src.dimensions: # Check if 'time' dimension exists
                dst.createDimension('time', len(src.dimensions['time']))
                out_time_var = dst.createVariable('time', src.variables['time'].dtype, ('time',), zlib=compress, complevel=complevel)
                out_time_var[:] = src.variables['time'][:]
                out_time_var.setncatts(src.variables['time'].__dict__)

            # Copy the crs variable with compression
            if 'crs' in src.variables: # Check if 'crs' variable exists
                out_crs_var = dst.createVariable('crs', src.variables['crs'].dtype, src.variables['crs'].dimensions, zlib=compress, complevel=complevel)
                out_crs_var[:] = src.variables['crs'][:]
                out_crs_var.setncatts(src.variables['crs'].__dict__)

            # Example: chunking along time, then 2160x1920 for y and x
            new_chunks = (1, 1920, 1920)  # Example: (time_chunk, y_chunk, x_chunk)

            # Clip and copy data variables with compression
            for var_name in data_vars:
                if var_name in src.variables:
                    var = src.variables[var_name]
                    # Determine dimensions for the output variable
                    var_dims = []
                    if 'time' in var.dimensions:
                        var_dims.append('time')
                    if 'lat' in var.dimensions:
                        var_dims.append('lat')
                    if 'lon' in var.dimensions:
                        var_dims.append('lon')

                    if var_dims: # Only proceed if it's a spatial/temporal variable
                        out_var = dst.createVariable(var_name, var.dtype, tuple(var_dims), zlib=compress, complevel=complevel, chunksizes=new_chunks)
                        out_var.setncatts(var.__dict__)

                        # Apply slicing based on the variable's dimensions
                        if 'time' in var.dimensions and 'lat' in var.dimensions and 'lon' in var.dimensions:
                            out_var[:] = var[:, lat_indices, lon_indices]
                        elif 'lat' in var.dimensions and 'lon' in var.dimensions:
                            out_var[:] = var[lat_indices, lon_indices]
                        else: # Handle other possible dimension orders if needed, or skip
                            print(f"Warning: Variable '{var_name}' has an unexpected combination of dimensions. Skipping clipping for this variable.")
                            # For simplicity, if it's not a (time, lat, lon) or (lat, lon) variable, copy it fully or skip
                            out_var[:] = var[:] # Copy full variable if dims not matched for clipping
                    else:
                        # If it's a scalar or 1D variable not related to time/lat/lon
                        out_var = dst.createVariable(var_name, var.dtype, var.dimensions, zlib=compress, complevel=complevel, chunksizes=new_chunks)
                        out_var.setncatts(var.__dict__)
                        out_var[:] = var[:]

            print(f"Successfully clipped '{input_nc_file}' and saved to '{output_nc_file}' with compression level {complevel}")

    except Exception as e:
        print(f"An error occurred: {e}")      
def main_modify_XML(date_str, ql_filename, destination_xml, params_dict, xml_file_path):
    """
    Main function to execute the XML parameter replacement.
    """

    # 3. Call the function to replace parameters and get the result.
    modified_xml = replace_xml_parameters_lxml(xml_file_path, params_dict, destination_xml)

    if modified_xml is not None:
        if modified_xml:
            print(modified_xml) # Print the modified XML
        else:
            print("Empty XML file.")
            
def replace_xml_parameters_lxml(xml_file_path, params_dict, output_file_path=None):
    """
    Replaces parameters within an XML file, identified by '$' prefix, with provided values.
    Outputs the file with UTF-8 BOM if output_file_path is provided.
    Uses lxml for better control over XML structure and namespace prefixes.

    Args:
        xml_file_path (str): Path to the XML file.
        params_dict (dict): Dictionary containing parameter names (without '$') as keys
                           and their corresponding replacement values as values.
        output_file_path (str, optional): Path to save the modified XML. If None,
                           the original file is overwritten. Defaults to None.

    Returns:
        str:  The modified XML content as a string. Returns an empty string
              if the file does not exist, is truly empty, or an error occurs during processing.
    """
    # Define the namespaces explicitly for lxml to use when serializing
    # This helps lxml preserve your preferred prefixes during output.
    # The 'None' key represents the default namespace.
    namespaces = {
        None: "http://www.isotc211.org/2005/gmd",
        "gco": "http://www.isotc211.org/2005/gco",
        "gml": "http://www.opengis.net/gml",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    }
    import lxml.etree as ET
    try:
        # Check if the file is truly empty (0 bytes) before parsing
        if not os.path.exists(xml_file_path) or os.path.getsize(xml_file_path) == 0:
            print(f"Error: XML file is empty or does not exist at {xml_file_path}")
            return ""

        # 1. Read the XML file using lxml's parser
        # lxml.etree.parse preserves comments and processing instructions by default.
        # remove_blank_text=True can help clean up significant whitespace between tags.
        parser = ET.XMLParser(remove_blank_text=False) # Keep this as False generally unless you want to strip all whitespace
        tree = ET.parse(xml_file_path, parser)
        root = tree.getroot()

    except FileNotFoundError:
        print(f"Error: File not found at {xml_file_path}")
        return ""
    except ET.ParseError as e:
        print(f"Error: XML parsing error in {xml_file_path}: {e}. Ensure it's valid XML.")
        return ""
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return ""

    def replace_text(element):
        """
        Helper function to recursively replace text within an XML element
        and its children.
        """
        if element.text:
            for key, value in params_dict.items():
                pattern = r'\$' + re.escape(key)
                element.text = re.sub(pattern, str(value), element.text)
        for child in element:
            replace_text(child)
        for attr_key, attr_value in element.attrib.items():
            for key, value in params_dict.items():
                pattern = r'\$' + re.escape(key)
                element.attrib[attr_key] = re.sub(pattern, str(value), attr_value)

    # 2. Iterate through the dictionary and replace the placeholders
    replace_text(root)

    # 3. Save the modified XML to a file or return as a string
    if output_file_path:
        try:
            # Generate the XML content as bytes using lxml's tostring
            # lxml's tostring preserves structure including comments/PIs from the tree object.
            # Using the 'namespaces' argument tells lxml to prefer these prefixes.
            xml_content_bytes = ET.tostring(
                tree, # Use 'tree' object here to preserve PIs/comments if they are in the tree
                encoding="utf-8",
                xml_declaration=True,
                pretty_print=True, # For nice formatting
                standalone=False, # Explicitly set standalone to 'no'
                #namespaces=namespaces # <--- This is the key argument for lxml
            )

            # Prepend the UTF-8 BOM
            bom = b'\xef\xbb\xbf'
            final_content_bytes = bom + xml_content_bytes

            with open(output_file_path, 'wb') as f: # Open in binary write mode
                f.write(final_content_bytes)
            print(f"Modified XML with UTF-8 BOM (using lxml) saved to {output_file_path}")
        except Exception as e:
            print(f"Error saving to {output_file_path}: {e}")
            return ""
    else:
        # If output_file_path is None, return the modified XML as a string (without BOM)
        return ET.tostring(
            root, # Use 'root' here if you don't need PIs/comments in the string output
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
            standalone=False,
            #namespaces=namespaces
        ).decode()

def thumbnail_view(filename, thumbFilename, colorTable):
     # parser = argparse.ArgumentParser(prog='create quicklook based on json config file')
    # parser.add_argument('--cfgFile', type=str, 
    #                     help='the input configuration file')
    

    qlDict = {
        "inFilename"    : filename, 
        "outFilename"   : thumbFilename,
        "colorTable"    : colorTable,
        "ql_Subsample"  : [5,5],
        "ql_Min"        : 0,
        "ql_Max"        : 210,
        "ql_NDV"        : 255,
        "ql_BandName"   : "LAI",
        "optional"      : ["Following parameters can optionally be used. src_min, srcMax taken into account if of type int"],
        #"ql_Additional_Format" : "PNG",
        #"src_Min"       : 0,
        #"src_Max"       : 99999,
        "gdal_open_option" : ["HONOUR_VALID_RANGE=FALSE"] #needed for DMP, if not available, defaults to  ["HONOUR_VALID_RANGE=FALSE"]
    }
    # args = parser.parse_args()

    try:
        # with open(args.cfgFile, encoding='utf-8') as fh:
        #     qlDict = json.load(fh)
        createQuicklook(qlDict)
        # err, errMsg = createQuicklook(qlDict)
        # if err:
        #     print(errMsg)
        #     raise Exception(errMsg)
        
    except:
        errMess = str(sys.exc_info()[1])
        errLine = sys.exc_info()[2].tb_lineno
        print(1,'[quicklook:' + str(errLine) + '] failed:\n' + errMess)
        sys.exit(1)

def zip_files_with_prefix(source_directory, zip_file_name, prefix=""):
    """
    Creates a zip file containing files from a source_directory,
    with an optional prefix for each file name within the zip.
    The directory itself is not included as a root in the zip.

    Args:
        source_directory (str): The path to the directory containing the files to zip.
        zip_file_name (str): The name of the zip file to create.
        prefix (str): The prefix to add to each file name within the zip.
    """
    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_directory):
            for file in files:
                file_path = os.path.join(root, file)
                # arcname is the name of the file within the zip archive
                # We want to remove the source_directory part from the path
                # and then add our custom prefix.
                relative_path = os.path.relpath(file_path, source_directory)
                arcname = f"{prefix}{relative_path}"
                zipf.write(file_path, arcname)
    print(f"Zip file '{zip_file_name}' created successfully with prefixed files.")


if __name__ == "__main__":
    # Check if a directory argument was provided
    if len(sys.argv) < 2:
        print("Usage: python clip_clms_LAI_SOAM.py <input_directory>")
        sys.exit(1)

    # sys.argv[0] is the script name itself ('clip_clms_NDVI_AFRI.py')
    # sys.argv[1] will be the first argument (the input directory)
    filepathname = sys.argv[1]
    dir_in = "/home/eouser/clms/LAI"
    dir_out = "/home/eouser/clms/outputs/"
    # Ensure the directory path ends with a separator if it's not already
    if not dir_in.endswith(os.sep): dir_in += os.sep
    if not dir_out.endswith(os.sep): dir_out += os.sep
    desired_width = 26880
    desired_height = 26880
    origin_lat = 20.001488095238095
    origin_lon = -110.001488095238102
    var_name = 'LAI'
    version = 'V1.1.1'
    # date_fileslist = glob.glob(dir_in+'c_gls_'+var_name+'300*GLOBE_OLCI_'+version+'.nc')
    # for filepathname in date_fileslist:
    date_str = os.path.basename(filepathname).split("_")[3]
    directory_name = dir_out + date_str[0:8]
    os.makedirs(directory_name, exist_ok=True)
    identifier = 'urn:cgls:south-america:lai300_v1_333m:'+var_name+'300-RT0_'+date_str+'_SOAM_OLCI_'+version
    parent_identifier = 'urn:cgls:south-america:lai300_v1_333m'
    # 1. Split the filename by "_"
    dire, filename = os.path.split(filepathname)
    parts = filename.split("_")
    # 2. Find the index of the part containing "GLOBE" and replace it
    for i, part in enumerate(parts):
        if "GLOBE" in part:
            parts[i] = part.replace("GLOBE", "SOAM")
            break # Important: Exit the loop after replacing
    # 3. Join the parts back together with "_"
    output_file = os.path.join(directory_name,"_".join(parts))

    ##### CLIP NETCDF4 ####
    print('filename:' + str(filepathname) + ' output_file:\n' + output_file)
    clip_all_vars_netcdf4(filepathname, output_file, origin_lat=origin_lat, origin_lon=origin_lon, clip_width=desired_width, clip_height=desired_height, data_vars = ['LAI', 'LENGTH_AFTER', 'LENGTH_BEFORE', 'NOBS', 'QFLAG', 'RMSE'], identifier=identifier, parent_identifier=parent_identifier)

    ##### Thumbnail view ####
    parts = os.path.splitext(output_file)[0].split("_")
    # Insert "QL" after the date part (202207010000)
    if len(parts) > 3:  # Make sure there's a date part
        parts.insert(3, "QL")
    thumbFilename = "_".join(parts)
    print('thumbFilename:' + str(thumbFilename))
    colorTable = os.path.join(dir_in, "cgl_colorTable_LAI.txt")
    thumbnail_view(output_file, thumbFilename, colorTable)
    ## Remove the aux file
    aux_file = thumbFilename+".tiff.aux.xml"
    if os.path.exists(aux_file):
        try:
            os.remove(aux_file)
            print(f"File '{aux_file}' removed successfully.")
        except OSError as e:
            print(f"Error removing file '{aux_file}': {e}")
    else:
        print(f"File '{aux_file}' does not exist.")

    ##### XML ####
    destination_xml = os.path.join(directory_name,"c_gls_"+var_name+"300-RT0_PROD-DESC_"+date_str+"_SOAM_OLCI_"+version+".xml")
    today= datetime.today().strftime("%Y-%m-%d")
    datetime_object = datetime.strptime(date_str, "%Y%m%d%H%M")
    formatted_date = datetime_object.strftime("%Y-%m-%d")

    #time_coverage_start = datetime_object.strftime("%Y-%m-%dT%H:%M:%S") #datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S")
    time_coverage_end = datetime_object.strftime("%Y-%m-%dT23:59:59")
    #If the new date is greater than the last day of the month, use the last day of the month
    if datetime_object.day == 10: #new_date > last_day_of_month:
        start_date = datetime_object.replace(day=1)
    elif datetime_object.day == 20:
        start_date = datetime_object.replace(day=11)
    else:
        start_date = datetime_object.replace(day=21)
    time_coverage_start = start_date.strftime("%Y-%m-%dT00:00:00")
    ql_filename = thumbFilename.split('/')[-1]+".tiff"
    params_dict = {
        "identifier": "urn:cgls:south-america:lai300_v1_333m:"+var_name+"300-RT0_"+date_str+"_SOAM_OLCI_"+version,
        "parent_identifier": "urn:cgls:south-america:lai300_v1_333m",
        "process_date": today, #"2024-01-26",
        "rows": 26880,
        "cols": 26880,
        "roi_id": "SOAM",
        "roi_name": "South-America",
        "ul_lat": 20.0,
        "ul_lon": -110.0,
        "lr_lon": -30.0,
        "lr_lat": -60.0,
        "platform": "Sentinel-3",
        "sensor": "OLCI",
        "previous_product_identifier": "",
        "alternate_title": var_name+"300-RT0_"+date_str+"_SOAM_OLCI_"+version,
        "product_date": formatted_date,
        "time_coverage_start": time_coverage_start,
        "time_coverage_end": time_coverage_end,
        "product_version": version,
        "ql_filename": ql_filename
    }
    xml_file_path = os.path.join(dir_in, "CGLS_"+var_name+"300_V1_S3_ProductSet_PDF_SOAM.xml")
    main_modify_XML(date_str, ql_filename, destination_xml, params_dict, xml_file_path = xml_file_path )

    ### Zip the folder ###
    output_zip_name = os.path.join(dir_out,os.path.splitext(os.path.basename(output_file))[0])
    try:
        zip_files_with_prefix(directory_name, output_zip_name+'.zip', date_str[0:8]+'/')
        # zip_path = shutil.make_archive(output_zip_name, 'zip', root_dir=dir_in, base_dir=directory_name)
        # print(f"Folder '{directory_name}' successfully zipped to '{zip_path}'")
    except Exception as e:
        print(f"Error zipping folder: {e}")

    ## Clean up dummy folder (optional)
    shutil.rmtree(directory_name)

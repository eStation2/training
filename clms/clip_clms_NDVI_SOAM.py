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


def createQuicklook_new(qlDict):
    '''
    createQuicklook

        creates quicklook from input netCDF using gdal API

        The outFilename is always, appended with '.tiff' to generate a quicklook.tiff
        Optionally, a png file can be created.
        e.g. cgl_DMP-RT0_QL_201808100000_X18Y03_S3_V1.1.rc1.png

        The output type of a quicklook is ALWAYS byte (gdal.GDT_Byte).

        src_Type, src_min, src_max are by default derived from the input dataset.

    Params:
    ------
    qlDict: dict
        Configuration dictionary holding following settings:
        mandatory:
            inFilename: str
                Path / name of input netCDF file.
            outFilename: str
                Path / basename of output file - without filename extension.
            colorTable: str
                Path / name of the colorTable (same file content as used to append to vrt file).
            ql_BandName: str
                The bandName to extract (e.g., "NDVI").
            ql_Subsample: list of int
                The subsampling factor to be used, e.g., [5,5] for 5x reduction.
                (This means output will be 1/5th the size, i.e., 20% width/height).
            ql_NDV: int 0...255
                The no_data_value to be used for the quicklook image.
            ql_Min, ql_Max: int 0..255
                The valid_range (minimum, maximum) data value for the quicklook image
                after scaling to 0-255.

        optional:
            ql_Additional_Format: str
                # QL.tif will be generated anyhow.
                # Additionally 'PNG' can be created.
                # For now, no other additional types supported.
            src_Min, src_Max: int
                Defining the valid_range of the input_band. Default taken from 'valid_range' attribute.
                Used to overrule input netCDF settings.
            ql_QFLAG_BandName: str
                The name of the QFLAG band to read (e.g., "QFLAG").
            ql_QFLAG_Value: int
                The specific value in the QFLAG band to match (e.g., 128).
            ql_NDVI_Value_For_QFLAG: int
                The NDVI value (in the output 0-255 range) to assign where QFLAG matches (e.g., 254).

    Returns:
    -------
    True, '' if OK
    False, 'errorMessage' if an error occurred
    '''

    try:
        # 1. Verify input parameter values
        if (not qlDict['ql_NDV'] in range(0, 256)) or \
                (not qlDict['ql_Min'] in range(0, 256)) or \
                (not qlDict['ql_Max'] in range(0, 256)):
            return False, 'Error createQuicklook: ql_ parameters (NDV, Min, Max) not within the expected output gdal.GDT_Byte range (0-255).'

        if not isinstance(qlDict['ql_Subsample'], list) or len(qlDict['ql_Subsample']) != 2 or \
                not all(isinstance(x, int) and x > 0 for x in qlDict['ql_Subsample']):
            return False, 'Error createQuicklook: ql_Subsample should be a list of two positive integers, e.g., [5,5].'

        if not os.path.exists(qlDict['inFilename']):
            return False, f'Error createQuicklook: Input file not found: {qlDict["inFilename"]}'

        # 2. Check if QFLAG modification is requested
        apply_qflag_modification = False
        if all(k in qlDict for k in ['ql_QFLAG_BandName', 'ql_QFLAG_Value', 'ql_NDVI_Value_For_QFLAG']):
            apply_qflag_modification = True
            print(
                f"QFLAG modification requested: Setting {qlDict['ql_BandName']} to {qlDict['ql_NDVI_Value_For_QFLAG']} where {qlDict['ql_QFLAG_BandName']} == {qlDict['ql_QFLAG_Value']}.")

        # 3. Open primary dataset and bands
        src_ds = gdal.Open(qlDict['inFilename'], gdal.GA_ReadOnly)
        if src_ds is None:
            return False, f'Error createQuicklook: Failed to open input NetCDF file: {qlDict["inFilename"]}'

        try:
            # Open the primary band (e.g., NDVI) as a subdataset
            ndvi_subdataset_path = f'NETCDF:"{qlDict["inFilename"]}":{qlDict["ql_BandName"]}'
            # Use OpenEx with HONOUR_VALID_RANGE=FALSE to read all values, including flags
            src_ds_ndvi_band = gdal.OpenEx(ndvi_subdataset_path, gdal.GA_ReadOnly,
                                           open_options=['HONOUR_VALID_RANGE=FALSE'])

            if src_ds_ndvi_band is None:
                return False, f'Error createQuicklook: Failed to open band "{qlDict["ql_BandName"]}" from {qlDict["inFilename"]}'

            ndvi_band = src_ds_ndvi_band.GetRasterBand(1)
            original_src_NDV = ndvi_band.GetNoDataValue()  # Keep original no-data value

            # Read metadata for valid_range
            src_bandMeta = ndvi_band.GetMetadata()
            src_Min, src_Max = None, None
            if 'valid_range' in src_bandMeta:
                validRange = src_bandMeta['valid_range'].strip('{').strip('}').split(',')
                # Convert to float for safe interpolation, handling potential float ranges
                validRange = list(map(np.float64, validRange))
                [src_Min, src_Max] = validRange

            # Read original NDVI data as float64 for precise manipulation
            original_ndvi_data_array = ndvi_band.ReadAsArray() #.astype(np.float64)

            # Get geotransform, projection, and dimensions from the primary band
            geotransform = src_ds_ndvi_band.GetGeoTransform()
            projection = src_ds_ndvi_band.GetProjection()
            width = src_ds_ndvi_band.RasterXSize
            height = src_ds_ndvi_band.RasterYSize

            # Close primary band subdataset (GDAL handles closing when objects go out of scope,
            # but explicit closure can be good practice with OpenEx)
            src_ds_ndvi_band = None

            qflag_data_array = None
            if apply_qflag_modification:
                # Open the QFLAG band
                qflag_subdataset_path = f'NETCDF:"{qlDict["inFilename"]}":{qlDict["ql_QFLAG_BandName"]}'
                src_ds_qflag_band = gdal.OpenEx(qflag_subdataset_path, gdal.GA_ReadOnly)

                if src_ds_qflag_band is None:
                    print(
                        f'Warning: Could not open QFLAG band "{qlDict["ql_QFLAG_BandName"]}". QFLAG modification will be skipped.')
                    apply_qflag_modification = False  # Disable QFLAG modification if band is not found
                else:
                    qflag_data_array = src_ds_qflag_band.GetRasterBand(1).ReadAsArray()
                    src_ds_qflag_band = None  # Close QFLAG band subdataset

            # Close the main dataset
            src_ds = None

        except Exception as e:
            # Catch exceptions during band opening/reading
            return False, f'Error createQuicklook: Error reading bands from {qlDict["inFilename"]}: {e}'

        # 4. Apply QFLAG modification if enabled and QFLAG data is available
        processed_ndvi_data_array = original_ndvi_data_array.copy()  # Work on a copy

        if apply_qflag_modification and qflag_data_array is not None:
            # Ensure QFLAG data has the same shape as NDVI data for element-wise operations
            if processed_ndvi_data_array.shape == qflag_data_array.shape:
                # Set NDVI value where QFLAG matches the specified value
                mask = (qflag_data_array == qlDict['ql_QFLAG_Value'])
                processed_ndvi_data_array[mask] = qlDict['ql_NDVI_Value_For_QFLAG']
                print(f"Applied QFLAG modification: Set {qlDict['ql_NDVI_Value_For_QFLAG']} for {np.sum(mask)} pixels.")
            else:
                print(
                    f"Warning: QFLAG band shape {qflag_data_array.shape} does not match NDVI band shape {processed_ndvi_data_array.shape}. QFLAG modification skipped.")

        # 5. Overrule src_Min/src_Max from qlDict if provided
        if 'src_Min' in qlDict and isinstance(qlDict['src_Min'], (int, float)):
            print(f'createQuicklook: Overruling input src_Min {src_Min} with cfg {qlDict["src_Min"]}')
            src_Min = qlDict['src_Min']
        if 'src_Max' in qlDict and isinstance(qlDict['src_Max'], (int, float)):
            print(f'createQuicklook: Overruling input src_Max {src_Max} with cfg {qlDict["src_Max"]}')
            src_Max = qlDict['src_Max']

        # Ensure src_Min and src_Max are defined for scaling
        if src_Min is None or src_Max is None:
            return False, 'Error createQuicklook: Cannot determine input band valid range (src_Min/src_Max). Provide "src_Min" and "src_Max" in qlDict or ensure "valid_range" attribute exists in NetCDF.'

        # # 6. Scale data to the 0-255 range using linear interpolation (np.interp)
        # scaled_ndvi_data = np.interp(processed_ndvi_data_array,
        #                              [src_Min, src_Max],
        #                              [qlDict['ql_Min'], qlDict['ql_Max']])
        #
        # # Clamp values to the output byte range (0-255)
        # scaled_ndvi_data = np.clip(scaled_ndvi_data, 0, 255)  # Keep as float for now, will cast later

        # 7. Apply NoDataValue: Set pixels that were originally NoData to the output ql_NDV
        # if original_src_NDV is not None:
        #     no_data_mask = (original_ndvi_data_array == original_src_NDV)
        #     processed_ndvi_data_array[no_data_mask] = qlDict['ql_NDV']

        # Convert the final scaled and no-data handled array to uint8
        # scaled_ndvi_data = scaled_ndvi_data.astype(np.uint8)

        # 8. Create an in-memory GDAL dataset from the processed NumPy array
        mem_driver = gdal.GetDriverByName('MEM')
        # Create a new in-memory dataset with the processed data, as GDT_Byte
        mem_ds = mem_driver.Create('', width, height, 1, gdal.GDT_Byte)
        mem_ds.SetGeoTransform(geotransform)
        mem_ds.SetProjection(projection)
        mem_ds.GetRasterBand(1).WriteArray(processed_ndvi_data_array)
        mem_ds.GetRasterBand(1).SetNoDataValue(qlDict['ql_NDV'])  # Set NoDataValue for the output band

        #create output
        ql_Type         = gdal.GDT_Byte
        #GIOG-1358: PROFILE=GeoTIFF removes scale and offset TAGS from GeoTIFF's
        creationOptions = ['COMPRESS=LZW', 'PROFILE=GeoTIFF']
        openOptions     = ['HONOUR_VALID_RANGE=FALSE']
        #GIOG-1601: DMP quicklook needs flag values to be set to no_data
        if 'gdal_open_option' in qlDict:
            openOptions = qlDict['gdal_open_option']

        # Output filenames
        qlTifFilename = qlDict['outFilename'] + '.tiff'
        qlPngFilename = qlDict['outFilename'] + '.png'

        # 9. Translate from the in-memory dataset to GTIFF
        # Calculate percentage for subsampling based on factor
        subsample_width_pct = 100.0 / qlDict['ql_Subsample'][0]
        subsample_height_pct = 100.0 / qlDict['ql_Subsample'][1]

        translateOptions_tif = gdal.TranslateOptions(
            format='GTIFF',
            widthPct=qlDict['ql_Subsample'][0],#subsample_width_pct,
            heightPct=qlDict['ql_Subsample'][1],#subsample_height_pct,
            creationOptions=creationOptions
            # outputType, scaleParams, noData are not needed here as they were handled in NumPy
        )
        # # translate subdataset to GTIFF and scale immediate to QL output format
        # translateOptions = gdal.TranslateOptions(format='GTIFF', outputType=ql_Type, bandList=[1],
        #                                          widthPct=qlDict['ql_Subsample'][0],
        #                                          heightPct=qlDict['ql_Subsample'][0],
        #                                          creationOptions=creationOptions, scaleParams=scaleParams,
        #                                          noData=qlDict['ql_NDV'])
        ## GIOG-1358: Do not interprete the NetCDF Valid range, otherwise all flags will be no_data
        # src_ds_sd = gdal.OpenEx(subdataset, gdal.GA_ReadOnly, open_options=openOptions)
        # gdal.Translate(qlTifFilename, src_ds_sd, options=translateOptions)
        # src_ds_sd = None
        gdal.Translate(qlTifFilename, mem_ds, options=translateOptions_tif)
        mem_ds = None  # Close the in-memory dataset to free resources


        # remove metadata
        # ERROR 6: The PNG driver does not support update access to existing datasets.
        dst_ds = gdal.Open(qlTifFilename, gdal.GA_Update)
        dst_ds.SetMetadata({})
        dst_ds.GetRasterBand(1).SetMetadata({})

        # 10. Remove metadata and add color table to the generated TIFF
        dst_ds = gdal.Open(qlTifFilename, gdal.GA_Update)
        if dst_ds:  # Check if open was successful
            dst_ds.SetMetadata({})  # Clear dataset level metadata
            dst_ds.GetRasterBand(1).SetMetadata({})  # Clear band level metadata

            # Add colorTable if specified and file exists
            if os.path.exists(qlDict['colorTable']):
                ct = createColorTable(qlDict['colorTable'])
                dst_ds.GetRasterBand(1).SetColorTable(ct)
            dst_ds.FlushCache()  # Ensure changes are written to disk
            dst_ds = None
        else:
            print(f"Warning: Could not open {qlTifFilename} for metadata/color table update. Skipping this step.")

        # 11. Translate to additional format (PNG) if requested
        if 'ql_Additional_Format' in qlDict:
            if qlDict['ql_Additional_Format'] != 'PNG':
                return False, 'Error createQuicklook: ql_Additional_Format currently only supports "PNG".'

            translateOptions_png = gdal.TranslateOptions(format='PNG', bandList=[1])
            gdal.Translate(qlPngFilename, qlTifFilename, options=translateOptions_png)

        return True, ''  # Return True for success, empty message

    except Exception as e:
        import traceback
        err_type, err_value, tb = sys.exc_info()
        # Get the line number of the error
        filename_err, errLine, func_name, text = traceback.extract_tb(tb)[-1]

        print(f'Error createQuicklook in file {filename_err} at line {errLine} in function {func_name}: \n{err_value}')
        return False, f'Error createQuicklook at line {errLine}: {err_value}'


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
    Specifically adapted for the structure of c_gls_NDVI300 data.
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

            # Clip and copy data variables with compression
            for var_name in data_vars:
                # Example: chunking along time, then 2160x1920 for y and x
                new_chunks = (1, 1920, 1920)  # Example: (time_chunk, y_chunk, x_chunk)
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

                    if var_name == "NDVI_unc": new_chunks = (1, 1415, 1415)
                    if var_dims: # Only proceed if it's a spatial/temporal variable
                        out_var = dst.createVariable(var_name, var.dtype, tuple(var_dims), zlib=compress, complevel=complevel, chunksizes=new_chunks)
                        out_var.setncatts(var.__dict__)
                        # for attr_name in var.ncattrs():
                        #     if attr_name not in ['valid_min', 'valid_max','valid_range']:
                        #         out_var.setncattr(attr_name, var.getncattr(attr_name))
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
def main_modify_XML(date_str, ql_filename, destination_xml):
    """
    Main function to execute the XML parameter replacement.
    """
    # date_str = "202207010000"
    datetime_object = datetime.strptime(date_str, "%Y%m%d%H%M")
    formatted_date = datetime_object.strftime("%Y-%m-%d")
    today= datetime.today().strftime("%Y-%m-%d")
    time_coverage_start = datetime_object.strftime("%Y-%m-%dT%H:%M:%S") #datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S")
    # Add 10 days
    new_date = datetime_object + relativedelta(days=+9)
    # Get the last day of the month
    last_day_of_month = datetime_object.replace(day=monthrange(datetime_object.year, datetime_object.month)[1])
    #If the new date is greater than the last day of the month, use the last day of the month
    if datetime_object.day == 21: #new_date > last_day_of_month:
        new_date = last_day_of_month
    time_coverage_end = new_date.strftime("%Y-%m-%dT23:59:59")
    # 1. Define the path to your XML file
    xml_file_path = "CGLS_NDVI300_V2_S3_ProductSet_PDF_SOAM.xml"
    #destination_xml = "c_gls_NDVI300_PROD-DESC_"+date_str+"_AFRI_OLCI_V2.0.1.xml"  # Replace with your desired destination file
    # copy_xml_file(source_xml, destination_xml)
    # ql_filename = ""
    # 2.  Define the dictionary of parameters and their values.
    #     Important:  The keys should match the parameter names *without* the '$' prefix.
    params_dict = {
        "identifier": "urn:cgls:continents:ndvi300_v2_333m:NDVI300_"+date_str+"_SOAM_OLCI_V2.0.1",
        "parent_identifier": "urn:cgls:continents:ndvi300_v2_333m",
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
        "alternate_title": "NDVI300_"+date_str+"_SOAM_OLCI_V2.0.1",
        "product_date": formatted_date,
        "time_coverage_start": time_coverage_start,
        "time_coverage_end": time_coverage_end,
        "product_version": "V2.0.1",
        "ql_filename": ql_filename
        # "xlink_href": "https://example.com/data",
        # "lineage_statement": "Processed using XYZ algorithm",
        # "transfer_size": 1234.56,
        # "file_size": 2345.67
        # Add all the other parameters here
    }

    # 3. Call the function to replace parameters and get the result.
    modified_xml = replace_xml_parameters(xml_file_path, params_dict, destination_xml)

    if modified_xml is not None:
        if modified_xml:
            print(modified_xml) # Print the modified XML
        else:
            print("Empty XML file.")
            
def thumbnail_view(filename, thumbFilename):
     # parser = argparse.ArgumentParser(prog='create quicklook based on json config file')
    # parser.add_argument('--cfgFile', type=str, 
    #                     help='the input configuration file')
    

    qlDict = {
        "inFilename"    : filename, #"clipped_c_gls_NDVI300_202207010000_AFRI_OLCI_V2.0.1.nc",
        "outFilename"   : thumbFilename, #"clipped_c_gls_NDVI300_QL_202207010000_AFRI_OLCI_V2.0.1",
        "colorTable"    : "ColorTable_NDVI300_V2.txt",  #"/data/ingest/clms/NDVI/ColorTable_NDVI300_V2.txt",
        "ql_Subsample"  : [5,5],
        "ql_Min"        : 0,
        "ql_Max"        : 255,
        "ql_NDV"        : 255,
        "ql_BandName"   : "NDVI",
        "ql_QFLAG_BandName": "QFLAG",       # Specify the QFLAG band name
        "ql_QFLAG_Value": 128,              # The QFLAG value to look for
        "ql_NDVI_Value_For_QFLAG": 254,     # The NDVI value to set when QFLAG matches
        "optional"      : ["Following parameters can optionally be used. src_min, srcMax taken into account if of type int"],
        #"ql_Additional_Format" : "PNG",
        "src_Min"       : 0,
        "src_Max"       : 255,
        "gdal_open_option" : ["HONOUR_VALID_RANGE=FALSE"] #needed for DMP, if not available, defaults to  ["HONOUR_VALID_RANGE=FALSE"]
    }
    # args = parser.parse_args()

    try:
        # with open(args.cfgFile, encoding='utf-8') as fh:
        #     qlDict = json.load(fh)
        createQuicklook(qlDict)
        # createQuicklook_new(qlDict)
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


# if __name__ == "__main__":
#     output_file="/data/ingest/clms/NDVI/c_gls_NDVI300_202505010000_AFRI_OLCI_V2.0.1.nc"
#     thumbFilename="/data/ingest/clms/NDVI/255_c_gls_NDVI300_QL_202505010000_AFRI_OLCI_V2.0.1"
#     thumbnail_view(output_file, thumbFilename)

if __name__ == "__main__":
    dir_in = "./"
    desired_width = 26880
    desired_height = 26880
    origin_lat = 20.001488095238095
    origin_lon = -110.001488095238102
    date_fileslist = glob.glob(dir_in+'c_gls_NDVI300*GLOBE_OLCI_V2.0.1.nc')
    var_name = 'NDVI'
    version = 'V2.0.1'
    for filepathname in date_fileslist:
        date_str = filepathname.split("_")[3]
        directory_name = dir_in+date_str[0:8]
        os.makedirs(directory_name, exist_ok=True)
        identifier = 'urn:cgls:continents:ndvi300_v2_333m:' + var_name + '300_' + date_str + '_SOAM_OLCI_' + version
        parent_identifier = 'urn:cgls:continents:ndvi300_v2_333m'
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
        clip_all_vars_netcdf4(filepathname, output_file, origin_lat=origin_lat, origin_lon=origin_lon,
                              clip_width=desired_width, clip_height=desired_height, data_vars = ['NDVI', 'NDVI_unc', 'LENGTH_BEFORE', 'NOBS', 'QFLAG'], identifier=identifier, parent_identifier=parent_identifier)
        #clip_to_pixel_extent_netcdf4(filename, output_file, extent_str, desired_width, desired_height)
        ##### Thumbnail view ####
        parts = os.path.splitext(output_file)[0].split("_")
        # Insert "QL" after the date part (202207010000)
        if len(parts) > 3:  # Make sure there's a date part
            parts.insert(3, "QL")
        thumbFilename = "_".join(parts)
        print('thumbFilename:' + str(thumbFilename))
        thumbnail_view(output_file, thumbFilename)
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
        destination_xml = os.path.join(directory_name,"c_gls_NDVI300_PROD-DESC_"+date_str+"_SOAM_OLCI_V2.0.1.xml")
        ql_filename = thumbFilename.split('/')[-1]+".tiff"
        ##### XML ####
        main_modify_XML(date_str, ql_filename, destination_xml)

        #### Zip the folder ###
        output_zip_name = os.path.splitext(os.path.basename(output_file))[0]
        try:
            zip_files_with_prefix(directory_name, output_zip_name+'.zip', date_str[0:8]+'/')
            # zip_path = shutil.make_archive(output_zip_name, 'zip', root_dir=dir_in, base_dir=directory_name)
            # print(f"Folder '{directory_name}' successfully zipped to '{zip_path}'")
        except Exception as e:
            print(f"Error zipping folder: {e}")

        # Clean up dummy folder (optional)
        shutil.rmtree(directory_name)

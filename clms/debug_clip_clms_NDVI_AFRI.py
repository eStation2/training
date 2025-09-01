import glob, os
from apps.tools.clms.clip_clms_NDVI_AFRI import clip_all_vars_netcdf4

# dir_in = "/data/ingest/clms/NDVI/"
# desired_width = 30240
# desired_height = 26880
# origin_lat = 40.0014880952380949
# origin_lon = -30.0014880952380949
# date_fileslist = glob.glob(dir_in + 'c_gls_NDVI300*.nc')
# var_name = 'NDVI'
# version = 'V1.1.1'
# for filepathname in date_fileslist:
#     date_str = filepathname.split("_")[3]
#     directory_name = dir_in + date_str[0:8]
#     os.makedirs(directory_name, exist_ok=True)
#     # 1. Split the filename by "_"
#     dire, filename = os.path.split(filepathname)
#     parts = filename.split("_")
#     # 2. Find the index of the part containing "GLOBE" and replace it
#     for i, part in enumerate(parts):
#         if "GLOBE" in part:
#             parts[i] = part.replace("GLOBE", "AFRI")
#             break  # Important: Exit the loop after replacing
#     # 3. Join the parts back together with "_"
#     output_file = os.path.join(dire, "_".join(parts))
#
#     ##### CLIP NETCDF4 ####
#     identifier = 'urn:cgls:continents:ndvi300_v2_333m:' + var_name + '300_' + date_str + '_AFRI_OLCI_' + version,
#     parent_identifier = 'urn:cgls:continents:ndvi300_v2_333m',
#     print('filename:' + str(filepathname) + ' output_file:\n' + output_file)
#     clip_all_vars_netcdf4(filepathname, output_file, origin_lat=origin_lat, origin_lon=origin_lon,
#                           clip_width=desired_width, clip_height=desired_height, identifier=identifier,
#                           parent_identifier=parent_identifier)
from lib.python import metadata as md
from lib.python.image_proc import raster_image_math
def checkIngestedFile(ref_file, newly_computed_file, fast=False, netcdf=True, var="LAI"):
    # Given the all files keys (date, prod, sprod, ...) finds out:
    # -> the product just ingested in the tmp_dir (see setUp)
    # -> the product in refs_output
    # Assess if the products are equal/equivalent
    result = False
    array_equal = False
    # Compare the files by using gdal_info objects
    if os.path.exists(ref_file) and os.path.exists(newly_computed_file):
        gdal_info_ref = md.GdalInfo()
        gdal_info_ref.get_gdalinfo(ref_file)
        gdal_info_new = md.GdalInfo()
        gdal_info_new.get_gdalinfo(newly_computed_file)
        equal = gdal_info_new.compare_gdalinfo(gdal_info_ref)

        if not equal:
            print("Warning: the NETCDF "+str(netcdf)+"files metadata are different for "+var)

        if netcdf:
            # Check the raster array compare
            array_equal = raster_image_math.compare_two_raster_array("NETCDF:"+ref_file+":"+var, "NETCDF:"+newly_computed_file+":"+var, fast=fast)
            if not array_equal:
                print("Warning: the NETCDF files contents are different for "+var)
        else:
            # Check the raster array compare
            array_equal = raster_image_math.compare_two_raster_array(ref_file, newly_computed_file, fast=fast)
            if not array_equal:
                print("Warning: the GTIFF files contents are different for "+var)

        if array_equal:
            result = True

    return result

for var in ["FCOVER"]:  #"DMP","FAPAR", "LAI", "FCOVER"
    netcdf = True
    directory_name = "/data/ingest/clms/"+var
    # if netcdf:
    if var != "NDVI":
        ref_file = os.path.join(directory_name, "org_c_gls_"+var+"300-RT0_202505100000_AFRI_OLCI_V1.1.1.nc")
        newly_computed_file = os.path.join(directory_name, "c_gls_"+var+"300-RT0_202505100000_AFRI_OLCI_V1.1.1.nc")#
    else:
        ref_file = os.path.join(directory_name, "org_c_gls_"+var+"300_202505010000_AFRI_OLCI_V2.0.1.nc")
        newly_computed_file = os.path.join(directory_name, "c_gls_"+var+"300_202505010000_AFRI_OLCI_V2.0.1.nc")#
    # else:
    #     if var != "NDVI":
    #         ref_file = os.path.join(directory_name, "org_c_gls_" + var + "300-RT0_QL_202505100000_AFRI_OLCI_V1.1.1.tiff")
    #         newly_computed_file = os.path.join(directory_name,"c_gls_" + var + "300-RT0_QL_202505100000_AFRI_OLCI_V1.1.1.tiff")  #
    #     else:
    #         ref_file = os.path.join(directory_name,"org_c_gls_" + var + "300_QL_202505100000_AFRI_OLCI_V2.0.1.tiff")
    #         newly_computed_file = os.path.join(directory_name, "c_gls_" + var + "300_QL_202505100000_AFRI_OLCI_V2.0.1.tiff")  #
    checkIngestedFile(ref_file, newly_computed_file, fast=False, netcdf=netcdf, var="LENGTH_BEFORE")
    netcdf = False
    # if netcdf:
    #     if var != "NDVI":
    #         ref_file = os.path.join(directory_name, "org_c_gls_"+var+"300-RT0_202505100000_AFRI_OLCI_V1.1.1.nc")
    #         newly_computed_file = os.path.join(directory_name, "c_gls_"+var+"300-RT0_202505100000_AFRI_OLCI_V1.1.1.nc")#
    #     else:
    #         ref_file = os.path.join(directory_name, "org_c_gls_"+var+"300_202505100000_AFRI_OLCI_V2.0.1.nc")
    #         newly_computed_file = os.path.join(directory_name, "c_gls_"+var+"300_202505100000_AFRI_OLCI_V2.0.1.nc")#
    # else:
    if var != "NDVI":
        ref_file = os.path.join(directory_name, "org_c_gls_" + var + "300-RT0_QL_202505100000_AFRI_OLCI_V1.1.1.tiff")
        newly_computed_file = os.path.join(directory_name,"c_gls_" + var + "300-RT0_QL_202505100000_AFRI_OLCI_V1.1.1.tiff")  #
    else:
        ref_file = os.path.join(directory_name,"org_c_gls_" + var + "300_QL_202505010000_AFRI_OLCI_V2.0.1.tiff")
        newly_computed_file = os.path.join(directory_name, "c_gls_" + var + "300_QL_202505010000_AFRI_OLCI_V2.0.1.tiff")  #
    checkIngestedFile(ref_file, newly_computed_file, fast=False, netcdf=netcdf, var=var)
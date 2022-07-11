import hidrocl_paths as hcl
from hidroclabc import HidroCLVariable, mod13q1extractor

ndvi = HidroCLVariable('ndvi', hcl.veg_o_modis_ndvi_mean_b_d16_p0d)
evi = HidroCLVariable('evi_temporal', hcl.veg_o_modis_evi_mean_b_d16_p0d)
nbr = HidroCLVariable('nbr_temporal', hcl.veg_o_int_nbr_mean_b_d16_p0d)

modext = mod13q1extractor(ndvi,evi,nbr)

while True:
    try:
        modext.run_extraction()
    except:
        continue
    break
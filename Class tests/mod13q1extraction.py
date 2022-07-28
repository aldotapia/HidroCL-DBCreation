import hidrocl_paths as hcl
from hidroclabc import HidroCLVariable, mod13q1extractor

ndvi = HidroCLVariable('ndvi_temporal', hcl.veg_o_modis_ndvi_mean_b_d16_p0d, hcl.veg_o_modis_ndvi_mean_pc)
evi = HidroCLVariable('evi_temporal', hcl.veg_o_modis_evi_mean_b_d16_p0d, hcl.veg_o_modis_evi_mean_pc)
nbr = HidroCLVariable('nbr_temporal', hcl.veg_o_int_nbr_mean_b_d16_p0d, hcl.veg_o_int_nbr_mean_pc)

modext = mod13q1extractor(ndvi,evi,nbr)

modext.run_extraction()
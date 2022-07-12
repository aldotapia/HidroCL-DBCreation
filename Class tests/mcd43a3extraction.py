import hidrocl_paths as hcl
from hidroclabc import HidroCLVariable, mcd43a3extractor

albedomean = HidroCLVariable('albedomean', hcl.sun_o_modis_al_mean_b_d16_p0d)
albedo10 = HidroCLVariable('albedo10', hcl.sun_o_modis_al_p10_b_d16_p0d)
albedo25 = HidroCLVariable('albedo25', hcl.sun_o_modis_al_p25_b_d16_p0d)
albedomedian = HidroCLVariable('albedomedian', hcl.sun_o_modis_al_median_b_d16_p0d)
albedo75 = HidroCLVariable('albedo75', hcl.sun_o_modis_al_p75_b_d16_p0d)
albedo90 = HidroCLVariable('albedo90', hcl.sun_o_modis_al_p90_b_d16_p0d)

modext = mcd43a3extractor(albedomean,albedo10,albedo25,albedomedian,albedo75,albedo90)

modext.run_extraction()
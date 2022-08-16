library(readr)
library(dplyr)

source("hidrocl_paths.R")

f1 <- read_csv(camels,quote = "")

f1n <- read_csv(camels, n_max = 0)

names(f1) <- names(f1n)

df <- data.frame(ID = catchment_names)

f1 %>%
  select(gauge_id,
         area_km2,
         mean_elev,
         med_elev,
         max_elev,
         min_elev,
         mean_slope_perc,
         sur_rights_flow_m3s,
         gw_rights_flow_m3s,
         aridity_cr2met_1979_2010) %>%
  mutate(area_km2 = round(area_km2 * 10),
         mean_elev = round(mean_elev),
         mean_slope_perc = round(mean_slope_perc * 10),
         sur_rights_flow_m3s = round(sur_rights_flow_m3s * 1000),
         gw_rights_flow_m3s = round(gw_rights_flow_m3s * 1000),
         aridity_cr2met_1979_2010 = round(aridity_cr2met_1979_2010 * 100)) %>% 
  right_join(df, by = c("gauge_id" = "ID")) %>% 
  rename(ID = gauge_id,
         top_s_cam_area_tot_b_c_c = area_km2,
         top_s_cam_elev_mean_b_c_c = mean_elev,
         top_s_cam_elev_med_b_c_c = med_elev,
         top_s_cam_elev_max_b_c_c = max_elev,
         top_s_cam_elev_min_b_c_c = min_elev,
         top_s_cam_slo_mean_b_c_c = mean_slope_perc,
         hi_s_camels_sr_tot_b_y_c = sur_rights_flow_m3s,
         hi_s_camels_gwr_tot_b_y_c = gw_rights_flow_m3s,
         idx_s_camels_arcr2_fr_b_c_c = aridity_cr2met_1979_2010) %>% 
  write_delim(x = .,
              file = all_s_camels,
              delim = ",")
library(sf)
library(tidyverse)
library(exactextractr)
library(terra)
library(magrittr)

# get paths for this machine
source('hidrocl_paths.R')

# extract mean altitude from nasadem
ipg <- st_read(ipg_path)
nasadem <- rast(nasadem_path)
alt <- exact_extract(nasadem, ipg,'mean',progress = FALSE) 
ipg$alt <- round(alt)

# extract main N / S orientation with nasadem output
nasadem_orien_reclass <- rast(nasadem_orien_reclass_path)
orien <- exact_extract(nasadem_orien_reclass, ipg,'mode',progress = FALSE) 
ipg$orien <- orien

# compute estimed density for further computations and select fields
ipg %<>% mutate(DENS_STIMD = EQ_AGUAKM3/VOL_km3) %>% 
  select(CLASIFICA,DENS_STIMD, ESP_MED, alt, orien) %>% 
  st_zm(drop = T, what = 'ZM')

# save new IPG shapefie
write_sf(ipg,ipg_hidrocl)

# load HidroCL polygons
hidrocl <- read_sf(hidrocl_utm)

# create a list for drop results
result_list <- list()

for(i in seq_along(hidrocl$gauge_id)){
  # load hidrocl polygon i
  poly_temp <- hidrocl[i,]
  # compute central longitude
  lng <- (st_bbox(poly_temp)[1] + st_bbox(poly_temp)[3])/2
  # round and substract by 1 grade
  lng <- round(lng) - 1
  # create a custom CRS
  temp_crs <- paste0('+proj=tmerc +lat_0=0 +lon_0=',lng,' +k=0.9996   +x_0=500000 +y_0=0 +ellps=GRS80  +towgs84=0,0,0,0,0,0,0')
  # tranform temporal polygon to the same CRS than IPG
  poly_temp <- st_transform(poly_temp, 32719)
  # intersect IPG and polygon
  inter_temp <- st_intersection(ipg, poly_temp)
  # transform to custom CRS
  inter_temp <- st_transform(inter_temp, temp_crs)
  # compute area
  area_temp <- st_area(inter_temp)
  # drop units and create a new field
  inter_temp$area_m2 <- units::drop_units(area_temp)
  # create database per polygon
  inter_temp %>% st_drop_geometry() %>%
    mutate(class = ifelse(CLASIFICA == 'GLACIARETE','gt',
                          ifelse(CLASIFICA == 'GLACIAR ROCOSO',
                                 'rgl','gl'))) %>% 
    mutate(orien = ifelse(orien == 0,'N','S')) %>% 
    mutate(VOL_m3 = area_m2*ESP_MED, WatEq_m3 = VOL_m3*DENS_STIMD) %>% 
    select(class, orien, area_m2, WatEq_m3, alt) %>% 
    group_by(class, orien) %>%
    summarise(Area_m2 = sum(area_m2,na.rm = T),
              WatEq_m3 = sum(WatEq_m3, na.rm = T),
              Alt_msl = weighted.mean(alt,area_m2)) %>% 
    mutate(ID = hidrocl$gauge_id[i]) -> result_list[[i]]
  print(paste0(hidrocl$gauge_id[i], ' done. Iteration number ',i))
  if(dim(result_list[[i]])[1]==0){
    # create dummy data if the polygon doesn't have glaciers
    result_list[[i]] <- data.frame( class = 'gl', orien = 'N', Area_m2 = 0,
                                    WatEq_m3 = 0, Alt_msl = 0, ID = hidrocl$gauge_id[i])
  }
}

# collapse tables
result <- do.call('rbind.data.frame', result_list)

# change names for easy handle
names(result) <- c('class','orien','a_tot','we_tot','alt_mean','ID')

#create final database
result %>% ungroup() %>%
  mutate(a_tot = round(a_tot/1000000,2), we_tot = round(we_tot/1000000000,4),
         alt_mean = round(alt_mean)) %>% 
  pivot_wider(id_cols = ID,names_from = c(class, orien),
              values_from = c(a_tot,we_tot,alt_mean),
              names_glue = "gl_s_dga_{class}{.value}_{orien}_d_p0d",
              values_fill = 0) -> db

# save database
write_delim(db, file = gl_s_dga_many, delim = ',')
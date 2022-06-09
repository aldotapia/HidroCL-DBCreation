#! /usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#

library(sf)
library(tidyverse)
#library(magrittr)

# get paths for this machine
source('hidrocl_paths.R')
# get functions
source('Basin_indices.R')

# load file
catchments <- st_read(hidrocl_wgs84)

# create a list for drop results
result_list <- list()

for(i in seq_along(catchments$gauge_id)){
  # load hidrocl polygon i
  poly_temp <- catchments[i,]
  # compute central longitude
  lng <- (st_bbox(poly_temp)[1] + st_bbox(poly_temp)[3])/2
  # round and substract by 1 grade
  lng <- round(lng) - 1
  # create a custom CRS
  temp_crs <- paste0('+proj=tmerc +lat_0=-80 +lon_0=',lng,' +k=0.9996   +x_0=500000 +y_0=0 +ellps=GRS80  +towgs84=0,0,0,0,0,0,0')
  # transform to custom CRS
  poly_temp <- st_transform(poly_temp, temp_crs)
  # compute cr
  cc_v <- cc(poly_temp)
  # compute cr
  cr_v <- cr(poly_temp)
  # dataframe for db
  result_list[[i]] <- data.frame(ID = poly_temp$gauge_id,
                                top_s_dem_com_fr_b_c_c = cc_v,
                                top_s_dem_cc_fr_b_c_c = cr_v)
}

# collapse tables
result <- do.call('rbind.data.frame', result_list)

# save database
write_delim(result, file = top_s_dem_many, delim = ',')

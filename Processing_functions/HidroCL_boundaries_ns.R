#! /usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# Script for processing north-south face
# by HidroCL boundaries

library(sf)
library(tidyverse)

# get paths for this machine
source('hidrocl_paths.R')

catchments <- st_read(hidrocl_sinusoidal)
asp <- st_read(aspect_vector)

# reproject to sinusoidal
asp <- st_transform(asp,crs = crs(catchments))

# clean geometries
asp <- st_make_valid(asp)

# create lists for storing data
asp_north <- list()
asp_south <- list()

# loop trough catchemnts interpoling and saving by face
for(i in seq_along(catchments$gauge_id)){
  # iterate polygons
  temp <- catchments[i,]
  temp_in <- st_intersection(temp, asp)
  
  # save only polygons
  try({temp_in <- st_collection_extract(
    temp_in,
    type = c("POLYGON")
  )})
  
  temp_in %<>% group_by(gauge_id,gauge_name,gridcode) %>% summarise()
  
  # split by facing
  asp_north[[i]] <- temp_in[temp_in$gridcode == 0,]
  asp_south[[i]] <- temp_in[temp_in$gridcode == 1,]
}

# combine all polygons
v_north <- do.call(rbind.data.frame,asp_north)
v_south <- do.call(rbind.data.frame,asp_south)

# save results
st_write(v_north,hidrocl_north)
st_write(v_south,hidrocl_south)
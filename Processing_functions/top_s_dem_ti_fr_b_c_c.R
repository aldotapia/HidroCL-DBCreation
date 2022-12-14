#! /usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#

library(sf)
library(terra)
library(readr)
library(exactextractr)

# get paths for this machine
source('hidrocl_paths.R')

# load file
catchments <- st_read(hidrocl_wgs84)

# create a list for drop results
rs <- list.files(cti_hdma, full.names = T, pattern = ".tif$")

mos <- vrt(rs)

custom_mean <- function(values, coverage_fractions) {
  covf <- coverage_fractions[!is.na(values)]
  vals <- values[!is.na(values)]
  try(round(sum(vals * covf) / sum(covf)), silent = TRUE)
}

result <- exact_extract(x = mos,
                        y = catchments,
                        fun = custom_mean,
                        append_cols = "gauge_id")

names(result) <- c("ID","top_s_dem_ti_fr_b_c_c")

# save database
write_delim(result, file = top_s_dem_ti_fr_b_c_c, delim = ',')

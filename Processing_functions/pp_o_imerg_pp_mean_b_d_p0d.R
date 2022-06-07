#! /usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# ATTENTION!
# This script was made for extracting the weighted mean of pixels
# using a vector file with THE SAME CRS THAN THE RASTER FILE. The
# vector reprojection is skipped here in order to keep the memory
# usage as low as possible. Be aware than the field used for appending
# ID column to value column here is `gauge_id`

options(warn=-1)

f_args = commandArgs(trailingOnly=TRUE)
sfv <- f_args[1] # polygon for extraction
pth <- f_args[2] # raster for extraction
ptt <- f_args[3] # output file
out <- f_args[4] # output file

sfv <- sf::st_read(sfv)

rfiles <- list.files(path = pth, pattern = ptt, full.names = T)

rlist <- list()

for(i in seq_along(rfiles)){
  rlist[[i]] <- terra::rast(rfiles[i], lyrs = 6)
  terra::ext(rlist[[i]]) <- c(-180,180,-90,90)
  rlist[[i]] <- terra::crop(rlist[[i]],sfv)
}

custom_mean <- function(values, coverage_fractions){
  covf <- coverage_fractions[!is.na(values)]
  vals <- values[!is.na(values)]
  try(round(sum(vals*covf)/sum(covf)),silent = TRUE)
}

result <- try({exactextractr::exact_extract(x = terra::app(terra::rast(rlist),'sum')*5,
                                            y = sfv,
                                            fun = custom_mean,
                                            append_cols = 'gauge_id',
                                            progress = F)}, silent = TRUE)
terra::tmpFiles(remove = T)
write.table(x = result,file = out,sep = ',', row.names = F)
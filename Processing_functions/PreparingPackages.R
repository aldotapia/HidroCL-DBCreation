#! /usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# Functions to install required packages for
# further weighted extraction
#

options(warn=-1)

if(suppressMessages(require(terra))){
  print('terra already installed')
}else{
  print('Installing terra')
  install.packages('terra',dependencies = TRUE)
}
if(suppressMessages(require(sf))){
  print('sf already installed')
}else{
  print('Installing sf')
  install.packages('sf',dependencies = TRUE)
}
if(suppressMessages(require(exactextractr))){
  print('exactextractr already installed')
}else{
  print('Installing exactextractr')
  install.packages('exactextractr',dependencies = TRUE)
}

library(terra)

source('hidrocl_paths.R')

# load data
dem <- rast(x = nasadem_path)

# compute aspect
asp <- terrain(x = dem, v = "aspect")

# write results
writeRaster(x = asp,
            filename =  nasadem_aspect_path ,
            gdal = c("COMPRESS=LZW"))

# the processing follows in AspectToVector.py (ArcGIS Pro workflow)
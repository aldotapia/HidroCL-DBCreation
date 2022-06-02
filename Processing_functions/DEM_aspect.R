library(terra)

source('hidrocl_paths.R')

# load data
dem <- rast(x = nasadem_path)

# compute aspect
asp <- terrain(x = dem, v = "aspect")

# create a reclassification matrix
reclass_matrix <- matrix(c(0,90,0,90,270,1,270,360,0),
                         byrow = TRUE,
                         ncol = 3)

# reclass aspect for two classes: north and south
asp_reclass <- classify(x = asp,
                        rcl = reclass_matrix,
                        include.lowest = TRUE)  

# create a function for getting mode value
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv <- uniqv[!is.na(uniqv)]
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

# agregate pixels
asp_reclass_x8 <- aggregate(x = asp_reclass,
                            fact = 8,
                            fun = getmode)


# apply a focal window filter with mode
asp_reclass_x8_focal5 <- focal(x = asp_reclass_x8,
                               w = 5,
                               fun = getmode)

# write results
writeRaster(x = asp_reclass_x8_focal5,
            filename =  nasadem_aspect_path ,
            gdal = c("COMPRESS=LZW"))
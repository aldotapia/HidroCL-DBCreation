library(readr)
library(sf)
library(terra)
library(dplyr)

source("hidrocl_paths.R")

f1 <- read_csv(camels, quote = "")
f1n <- read_csv(camels, n_max = 0)
names(f1) <- names(f1n)

point_list <- list()

for (i in 1:dim(f1)[1]) {
  pnt <- st_sfc(st_point(as.matrix(f1[i,
                                      c("outlet_camels_lon",
                                        "outlet_camels_lat")]),
                         dim = "XY"))
  point_list[[i]] <- st_sf(pnt, ID = f1$gauge_id[i],
                           crs = "EPSG:4326")
}

pnts <- do.call(rbind.data.frame, point_list)

gsrs_r <- rast(gsrs)
gsrs_r <- clamp(gsrs_r,
                lower = 0,
                upper = 50,
                values = FALSE)

result <- extract(gsrs_r, vect(pnts))

result <- data.frame(ID = pnts$ID, deep = result[, 2])


data.frame(ID = catchment_names) %>%
  left_join(result, by = "ID") -> df

# nan points, extract with buffer
df[is.nan(df$deep), "deep"] <- pnts[is.nan(df$deep), ] %>%
  st_buffer(dist = 0.0001) %>%
  vect(.) %>%
  extract(gsrs_r, y = .) %>%
  .[, 2]

names(df) <- c("ID",
               "sf_s_daac_brd_tot_point_c_c")

write_delim(x = df,
            file = sf_s_daac_brd_tot_point_c_c,
            delim = ",")
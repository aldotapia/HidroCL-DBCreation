library(sf)
library(tidyverse)

source("hidrocl_paths.R")

v <- st_read(glhymps)
hidrocl <- st_read(hidrocl_utm)
hidrocl <- st_transform(hidrocl, crs(v))

result_list <- list()

for (i in seq_along(hidrocl$gauge_id)) {
  poly_temp <- hidrocl[i, ]
  inter_temp <- st_intersection(v, poly_temp)
  inter_temp <- st_transform(inter_temp,
                             "+init=epsg:4326")
  lng <- (st_bbox(inter_temp)[1] + st_bbox(inter_temp)[3]) / 2
  lng <- round(lng) - 1
  temp_crs <- paste0("+proj=tmerc +lat_0=0 +lon_0=",
                     lng,
                     " +k=0.9996   +x_0=500000 ",
                     "+y_0=0 +ellps=GRS80  +towgs84=0,0,0,0,0,0,0")
  inter_temp <- st_transform(inter_temp, temp_crs)
  area_temp <- st_area(inter_temp)
  inter_temp$area_m2 <- units::drop_units(area_temp)

  inter_temp %>%
    st_drop_geometry() %>%
    mutate(porosity = Porosity_x * area_m2,
           permeability = logK_Ice_x * area_m2) %>%
    summarise(porosity = sum(porosity, na.rm = TRUE),
              permeability = sum(permeability, na.rm = TRUE),
              area_m2 = sum(area_m2, na.rm = TRUE),
              gauge_id = first(gauge_id)) %>%
    mutate(porosity = porosity / area_m2,
           permeability = permeability / area_m2) %>%
    select(gauge_id, porosity, permeability) -> result_list[[i]]

  print(paste0(hidrocl$gauge_id[i], " done. Iteration number ", i))
  if (dim(result_list[[i]])[1] == 0) {
    result_list[[i]] <- data.frame(gauge_id = hidrocl$gauge_id[i],
                                   porosity = NA,
                                   permeability = NA)
  }
}

df <- do.call(rbind.data.frame,
              result_list)

names(df) <-  c("ID",
                "sf_s_glhym_por_mean_b_c_c",
                "sf_s_glhym_perm_mean_b_c_c")

df[, 2] <- round(df[, 2])
df[, 3] <- round(df[, 3])

write_delim(x = df,
            file = sf_s_glhym,
            delim = ",")

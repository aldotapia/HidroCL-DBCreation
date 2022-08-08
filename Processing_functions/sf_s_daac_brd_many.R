library(terra)
library(sf)
library(exactextractr)
library(dplyr)
library(readr)

source("hidrocl_paths.R")

gsrs_r <- rast(gsrs)
v <- read_sf(hidrocl_wgs84)

gsrs_r <- clamp(gsrs_r,
                lower = 0,
                upper = 50,
                values = FALSE)

result1 <- exact_extract(x = gsrs_r,
                         y = v,
                         fun = "quantile",
                         quantiles = c(0.1,
                                       0.25, 0.5, 0.75, 0.9),
                         append_cols = "gauge_id",
                         progress = F)

result2 <- exact_extract(x = gsrs_r,
                         y = v,
                         fun = "mean",
                         append_cols = "gauge_id",
                         progress = F)

result1 %>%
  left_join(result2,
            by = "gauge_id") -> df

df[, 2:7] <- round(df[, 2:7] * 100)

names(df) <- c("ID",
               "sf_s_daac_brd_p10_b_c_c",
               "sf_s_daac_brd_p25_b_c_c",
               "sf_s_daac_brd_p50_b_c_c",
               "sf_s_daac_brd_p75_b_c_c",
               "sf_s_daac_brd_p90_b_c_c",
               "sf_s_daac_brd_mean_b_c_c")

write_delim(x = df,
            file = sf_s_daac_brd,
            delim = ",")
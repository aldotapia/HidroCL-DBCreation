library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readr)

source("hidrocl_paths.R")

files <- list.files(pattern = ".csv", path = "path/to/SoilGrids/")

dict <- data.frame(parameter = c("bdod",
                                 "cfvo",
                                 "clay",
                                 "sand",
                                 "silt",
                                 "soc"),
                   variable = c("sbd",
                                "scf",
                                "clay",
                                "sand",
                                "silt",
                                "socc"))

files %>% map(function(x) {
  read.csv(x) %>% select(-gauge_name,
                         -separador,
                         -X,
                         -Train,
                         -area_km2)
}) %>%
  reduce(left_join, by = "gauge_id") %>%
  pivot_longer(-gauge_id) %>%
  mutate(layers = gsub(pattern = "cm_.*",
                       replacement = "",
                       x = name),
         layers = gsub(pattern = ".*_",
                       replacement = "",
                       x = layers)) %>%
  separate(layers, c("layerfrom",
                     "layerto")) %>%
  mutate(layerfrom = as.numeric(layerfrom),
         layerto = as.numeric(layerto),
         layerdiff = layerto - layerfrom,
         parameter = gsub(pattern = "_.*",
                          replacement = "",
                          x = name),
         stat = gsub(pattern = ".*_",
                     replacement = "",
                     x = name),
         fracvalue = value * layerdiff / 200) %>%
  separate(stat, c("stat",
                   "deletethen")) %>%
  group_by(gauge_id, parameter, stat) %>%
  summarise(value = round(sum(fracvalue)))  %>%
  ungroup() %>%
  left_join(dict,
            by = "parameter") %>%
  mutate(dbcode = paste0("sf_s_isric_",
                         variable,
                         "_",
                         stat,
                         "_b_c_c")) %>%
  select(gauge_id,
         dbcode,
         value) %>%
  pivot_wider(names_from = dbcode,
              values_from = value) %>%
  rename(ID = gauge_id) -> df

write_delim(x = df,
            file = sf_s_isric_all,
            delim = ",")
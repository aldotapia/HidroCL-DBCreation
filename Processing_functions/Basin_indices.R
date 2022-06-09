if(suppressMessages(require(sf))){
  print('sf installed')
}else{
  print('Installing sf library')
  install.packages('sf',dependencies = TRUE)
}
if(suppressMessages(require(lwgeom))){
  print('sf installed')
}else{
  print('Installing lwgeom library')
  install.packages('lwgeom',dependencies = TRUE)
}

# basin shape indices
cc <- function(ctch){
  # compactness coefficient (Gravelius, 1914)
  if(nrow(ctch) == 1){
    area_ <- sf::st_area(ctch)
    area_ <- units::drop_units(area_)
    perimeter_ <- lwgeom::st_perimeter(ctch)
    perimeter_ <- units::drop_units(perimeter_)
    
    return(round(0.2841*perimeter_/sqrt(area_),3))
  }else{
    print('Designed for one element at time (so far)')
  }
}

cr <- function(ctch){
  # circularity ratio (Miller, 1953)
  
  if(nrow(ctch) == 1){
    area_ <- sf::st_area(ctch)
    area_ <- units::drop_units(area_)
    perimeter_ <- lwgeom::st_perimeter(ctch)
    perimeter_ <- units::drop_units(perimeter_)
    
    return(round(12.57*area_/(perimeter_^2),3))
  }else{
    print('Designed for one element at time (so far)')
  }
}

# References:
#
# Gravelius H (1914) Grundriß der gesamten Gewâsserkunde, Band
#     1: Flußkunde (Compendium of Hydrol- ogy, vol. 1: Rivers,
#     in German). Goschen, Berlin, Germany
# 
# Miller VC (1953) A quantitative geomorphic study of drainage
#     basin characteristics in the Clinch Mountain Area, Virginia
#     and Tennessee. New York: Columbia University, Office of
#     Naval Research Project NR 389 - 042, Technical Report No. 3
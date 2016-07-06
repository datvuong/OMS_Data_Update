GetSupplierAddressRegion <- function(supplier, supplierAddressRegion) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  functionName <- "GetSOAddressRegion"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    addressRegionData_rev <- addressRegionData %>%
      select(id_customer_address_region,
             fk_country,
             code,
             name,
             customer_address_region_type,
             fk_customer_address_region) %>%
      mutate(fk_customer_address_region = as.integer(as.character(fk_customer_address_region)))
    addressRegionData_mapped <- addressRegionData_rev %>%
      filter(customer_address_region_type == 4) %>%
      select(sup_level_4_id_customer_address_region = id_customer_address_region,
             sup_level_4_code = code,
             sup_level_4_name = name,
             sup_level_4_customer_address_region_type = customer_address_region_type,
             sup_level_4_fk_customer_address_region = fk_customer_address_region)
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                            select(sup_level_3_id_customer_address_region = id_customer_address_region,
                                                   sup_level_3_code = code,
                                                   sup_level_3_name = name,
                                                   sup_level_3_customer_address_region_type = customer_address_region_type,
                                                   sup_level_3_fk_customer_address_region = fk_customer_address_region),
                                          by = c("sup_level_4_fk_customer_address_region" = "sup_level_3_id_customer_address_region"))
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                            select(sup_level_2_id_customer_address_region = id_customer_address_region,
                                                   sup_level_2_code = code,
                                                   sup_level_2_name = name,
                                                   sup_level_2_customer_address_region_type = customer_address_region_type,
                                                   sup_level_2_fk_customer_address_region = fk_customer_address_region),
                                          by = c("sup_level_3_fk_customer_address_region" = "sup_level_2_id_customer_address_region"))
    
    supplierAddressRegion <- left_join(supplier, addressRegionData_mapped,
                                 by = c("fk_country_region" = "sup_level_4_id_customer_address_region"))
    
    supplierAddressRegion
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}


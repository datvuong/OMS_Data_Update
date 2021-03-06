GetSOAddressRegion <- function(SOAddressData, addressRegionData) {
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
      filter(customer_address_region_type == 6) %>%
      select(id_customer_address_region,
             level_6_id_customer_address_region = id_customer_address_region,
             level_6_code = code,
             level_6_name = name,
             level_6_customer_address_region_type = customer_address_region_type,
             level_6_fk_customer_address_region = fk_customer_address_region)
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                            select(level_5_id_customer_address_region = id_customer_address_region,
                                                   level_5_code = code,
                                                   level_5_name = name,
                                                   level_5_customer_address_region_type = customer_address_region_type,
                                                   level_5_fk_customer_address_region = fk_customer_address_region),
                                          by = c("level_6_fk_customer_address_region" = "level_5_id_customer_address_region"))
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                      select(level_4_id_customer_address_region = id_customer_address_region,
                                             level_4_code = code,
                                             level_4_name = name,
                                             level_4_customer_address_region_type = customer_address_region_type,
                                             level_4_fk_customer_address_region = fk_customer_address_region),
                                    by = c("level_5_fk_customer_address_region" = "level_4_id_customer_address_region"))
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                            select(level_3_id_customer_address_region = id_customer_address_region,
                                                   level_3_code = code,
                                                   level_3_name = name,
                                                   level_3_customer_address_region_type = customer_address_region_type,
                                                   level_3_fk_customer_address_region = fk_customer_address_region),
                                          by = c("level_4_fk_customer_address_region" = "level_3_id_customer_address_region"))
    
    SOAddressRegion <- left_join(SOAddressData, addressRegionData_mapped,
                                 by = c("fk_customer_address_region" = "level_6_id_customer_address_region"))
    
    SOAddressRegion
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}


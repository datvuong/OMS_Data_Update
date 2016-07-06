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
      filter(customer_address_region_type == 4) %>%
      select(id_customer_address_region,
             level_4_id_customer_address_region = id_customer_address_region,
             level_4_code = code,
             level_4_name = name,
             level_4_customer_address_region_type = customer_address_region_type,
             level_4_fk_customer_address_region = fk_customer_address_region)
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                            select(level_3_id_customer_address_region = id_customer_address_region,
                                                   level_3_code = code,
                                                   level_3_name = name,
                                                   level_3_customer_address_region_type = customer_address_region_type,
                                                   level_3_fk_customer_address_region = fk_customer_address_region),
                                          by = c("level_4_fk_customer_address_region" = "level_3_id_customer_address_region"))
    
    addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                          addressRegionData_rev %>%
                                      select(level_2_id_customer_address_region = id_customer_address_region,
                                             level_2_code = code,
                                             level_2_name = name,
                                             level_2_customer_address_region_type = customer_address_region_type,
                                             level_2_fk_customer_address_region = fk_customer_address_region),
                                    by = c("level_3_fk_customer_address_region" = "level_2_id_customer_address_region"))
    
    SOAddressRegion <- left_join(SOAddressData, addressRegionData_mapped,
                                 by = c("fk_customer_address_region" = "level_4_id_customer_address_region"))
    
    SOAddressRegion
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}


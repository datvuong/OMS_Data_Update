UpdateSOIBaseData <- function(soiData, packageData, soiTimestampData,
                              skuData, SCSellerData) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(logging)
  })
  
  functionName <- "UpdateSOIBaseData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    skuData %<>%
      mutate(volumetricDimension = sum((package_length * package_width * package_height)))
    
    soiBasedData <- soiData %>%
      left_join(skuData %>%
                  select(sku, product_name, package_weight,
                         volumetricDimension), by = c("sku" = "sku")) %>%
      left_join(packageData %>%
                  select(tracking_number, package_number, fk_sales_order_item,
                         shipment_provider_name), by = c("id_sales_order_item" = "fk_sales_order_item")) %>%
      left_join(soiTimestampData, by = c("id_sales_order_item" = "fk_sales_order_item"))
    
    soiBasedData %<>%
      select(order_nr, id_sales_order_item, bob_id_sales_order_item,
             business_unit, payment_method, sku, product_name, 
             unit_price, paid_price, shipping_fee,
             shipping_surcharge, Item_Status, tracking_number, 
             package_number, shipment_provider_name, fk_marketplace_merchant,
             package_weight, volumetricDimension,
             RTS_Date = rts, Shipped_Date = shipped,
             Cancelled_Date = cancelled, Delivered_Date = delivered,
             fk_sales_order_address_shipping)
    
    soiBasedData %<>% 
      left_join(SCSellerData %>%
                  filter(!is.na(src_id)) %>%
                  select(src_id, Seller_Code, Seller, tax_class), by = c("fk_marketplace_merchant" = "src_id"))
    
    soiBasedData
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}
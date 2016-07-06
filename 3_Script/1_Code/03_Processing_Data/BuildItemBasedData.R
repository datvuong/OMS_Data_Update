BuildItemBasedData <- function(venture, startDate, monthsGoBack = 6) {

  functionName <- "BuildItemBasedData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  packageDataBased <- tryCatch({
    source("3_Script/1_Code/00_init.R")
    source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
    source("3_Script/1_Code/03_Processing_Data/UpdateSOITimeStamp.R")
    source("3_Script/1_Code/03_Processing_Data/GetSixMonthData.R")
    source("3_Script/1_Code/03_Processing_Data/UpdateSOIBaseData.R")
    source(file.path("3_Script/1_Code/03_Processing_Data",ventureShort , paste0(ventureShort, "_GetSOAddressRegion.R")))
    source("3_Script/1_Code/03_Processing_Data/BuildPackageData.R")
    
    load(file.path(ventureDataFolder, "oms_live", "oms_shipment_provider.RData"))
    load(file.path(ventureDataFolder, "skuData.RData"))
    load(file.path(ventureDataFolder, "oms_live", "oms_warehouse.RData"))
    
    startDate <- as.Date(startDate, format = "%Y-%m-%d")
    
    flog.info("Consolidate OMS Data", name = reportName)
    
    ##### History Timestamp Data #####
    soiHistoryData <- GetXMonthData(ventureShort = ventureShort,  dbName = "oms_live", tableName = "ims_sales_order_item_status_history", xMonth = 0,
                                    startDate = startDate, keyField = "id_sales_order_item_status_history")
    soiTimestampData <- UpdateSOITimeStamp(soiHistoryData)
    soiTimestampData %<>%
      rowwise() %>%
      mutate(havingData = any(!is.na(c(rts, shipped, cancelled, delivered, being_returned)))) %>%
      ungroup() %>%
      filter(havingData)
    rm(soiHistoryData)
    gc()
    ##### Sales Order Item Data #####
    salesOrderItem <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "ims_sales_order_item", xMonth = monthsGoBack,
                                    startDate = startDate, 
                                    columnsList = c("id_sales_order_item", "bob_id_sales_order_item", "fk_sales_order",
                                                    "fk_sales_order_item_status", "fk_marketplace_merchant", "unit_price",
                                                    "paid_price", "sku", "amount_paid", "shipping_fee", "shipping_discount_amount",
                                                    "shipping_surcharge", "fk_mwh_warehouse"),
                                    keyField = "id_sales_order_item")
    salesOrderItemBased <- right_join(salesOrderItem, soiTimestampData,
                                      by = c("id_sales_order_item" = "fk_sales_order_item"))
    rm(soiTimestampData, salesOrderItem)
    gc()
    ##### SKU Data #####
    salesOrderItemBased <- left_join(salesOrderItemBased, skuData,
                                     by = "sku")
    rm(skuData)
    salesOrderItemBased %<>%
      mutate(volumetricDimension = package_length * package_width * package_height)
    gc()
    ##### Package Item data #####
    packageItemData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "oms_package_item", xMonth = "All",
                                     startDate = startDate, 
                                     columnsList = c("fk_package", "fk_sales_order_item", "fk_inventory"),
                                     keyField = "id_package_item")
    salesOrderItemBased <- left_join(salesOrderItemBased, packageItemData,
                                     by = c("id_sales_order_item" = "fk_sales_order_item"))
    rm(packageItemData)
    gc()
    ##### Package dispatching data #####
    packageDispatchingData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "oms_package_dispatching", xMonth = monthsGoBack,
                                            startDate = startDate, 
                                            columnsList = c("id_package_dispatching", "fk_package", "fk_shipment_provider",
                                                            "tracking_number"),
                                            keyField = "id_package_dispatching")
    salesOrderItemBased <- left_join(salesOrderItemBased, packageDispatchingData,
                                     by = ("fk_package" = "fk_package"))
    rm(packageDispatchingData)
    gc()
    ##### SC Seller Data #####
    load(file.path(ventureDataFolder, "screport", "seller.RData"))
    salesOrderItemBased <- left_join(salesOrderItemBased, seller %>%
                                       filter(!is.na(src_id)) %>%
                                       select(src_id, Seller_Code = short_code,
                                              Seller = name, tax_class),
                                     by = c("fk_marketplace_merchant" = "src_id"))
    rm(seller)
    gc()
    
    ##### Sales Order Data #####
    salesOrder <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "ims_sales_order", xMonth = monthsGoBack,
                                startDate = startDate, 
                                columnsList = c("id_sales_order", "order_nr", "payment_method"),
                                keyField = "id_sales_order")
    salesOrderItemBased <- left_join(salesOrderItemBased, salesOrder,
                                  by = c("fk_sales_order" = "id_sales_order"))
    rm(salesOrder)
    gc()
    
    ##### Package Data #####
    packageNumberData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "oms_package", xMonth = monthsGoBack,
                                       startDate = startDate, 
                                       columnsList = c("id_package", "package_number"),
                                       keyField = "id_package")
    salesOrderItemBased <- left_join(salesOrderItemBased, packageNumberData,
                                  by = c("fk_package" = "id_package"))
    rm(packageNumberData)
    gc()
    ##### Shipment Provider Data #####
    salesOrderItemBased <- left_join(salesOrderItemBased, 
                                  oms_shipment_provider %>%
                                    select(id_shipment_provider, shipment_provider_name),
                                  by = c("fk_shipment_provider" = "id_shipment_provider"))
    
    
    ##### Remove unneeded columns #####
    salesOrderItemBased %<>%
      select(-c(package_length, package_width,
                package_height, fk_package, id_package_dispatching,
                fk_shipment_provider))
    
    salesOrderItemBased
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  salesOrderItemBased
}
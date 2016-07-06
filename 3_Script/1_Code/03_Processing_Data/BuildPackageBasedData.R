BuildPackageBasedData <- function(venture, startDate, monthsGoBack = 6) {

  functionName <- "BuildPackageBasedData"
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
    
    GetUniqueList <- function(list) {
      uniqueList <- unique(list)
      uniqueList
    }
    
    flog.info("Build Package Base - 1 ", name = reportName)
    packageData <- salesOrderItemBased %>%
      filter(!is.na(tracking_number)) %>%
      group_by(tracking_number) %>%
      mutate(itemsCount = n_distinct(id_sales_order_item))
    flog.info("Build Package Base - 2 ", name = reportName)
    packageData %<>%  
      mutate(paidPrice = sum(paid_price))
    packageData %<>%  
      mutate(unit_price = sum(unit_price))
    flog.info("Build Package Base - 3 ", name = reportName)
    packageData %<>%  
      mutate(shippingFee = sum(shipping_fee))
    flog.info("Build Package Base - 4 ", name = reportName)
    packageData %<>%  
      mutate(shippingSurcharge = sum(shipping_surcharge))
    flog.info("Build Package Base - 5 ", name = reportName)
    packageData %<>%  
      mutate(skus = paste(GetUniqueList(sku), collapse = "/"))
    flog.info("Build Package Base - 6 ", name = reportName)
    packageData %<>%  
      mutate(skus_names = paste(GetUniqueList(product_name), collapse = "/"))
    flog.info("Build Package Base - 7 ", name = reportName)
    packageData %<>%  
      mutate(actualWeight = sum(package_weight))
    flog.info("Build Package Base - 8 ", name = reportName)
    packageData %<>%  
      mutate(missingActualWeight = any(is.na(actualWeight)))
    flog.info("Build Package Base - 9 ", name = reportName)
    packageData %<>%  
      mutate(missingVolumetricDimension = any(is.na(volumetricDimension)))
    flog.info("Build Package Base - 10 ", name = reportName)
    packageData %<>%  
      mutate(Seller_Code = paste(GetUniqueList(Seller_Code), collapse = "/"))
    flog.info("Build Package Base - 11 ", name = reportName)
    packageData %<>%  
      mutate(Seller = paste(GetUniqueList(Seller), collapse = "/"))
    
    my_min <- function(x) {
      x_notNA <- x[!is.na(x)]
      if (length(x_notNA) == 0)
        x_notNA <- c(as.POSIXct(NA))
      return(min(x_notNA))
    }
    packageDataBased <- packageData %>%
      group_by(tracking_number) %>%
      mutate(rts = min(rts),
             shipped = my_min(shipped),
             cancelled = my_min(cancelled),
             delivered = my_min(delivered),
             being_returned = my_min(being_returned)) %>%
      ungroup() %>%
      filter(!duplicated(tracking_number))
    rm(packageData)
    gc()
    
    ##### Sales Order Data #####
    salesOrder <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "ims_sales_order", xMonth = monthsGoBack,
                                startDate = startDate, 
                                columnsList = c("id_sales_order", "order_nr", "payment_method"),
                                keyField = "id_sales_order")
    packageDataBased <- left_join(packageDataBased, salesOrder,
                                  by = c("fk_sales_order" = "id_sales_order"))
    rm(salesOrder)
    gc()
    ##### Address Region Data #####
    bobSalesOrder <- GetXMonthData(ventureShort = ventureShort, dbName = "bob_live", tableName = "sales_order", xMonth = monthsGoBack,
                                   startDate = startDate, 
                                   columnsList = c("order_nr", "fk_sales_order_address_shipping"),
                                   keyField = "order_nr")
    salesOrderAddress <- GetXMonthData(ventureShort = ventureShort, dbName = "bob_live", tableName = "sales_order_address", xMonth = "all",
                                       startDate = startDate, 
                                       columnsList = c("id_sales_order_address", "city", "postcode",
                                                       "fk_customer_address_region"),
                                       keyField = "id_sales_order_address")
    load(file.path(ventureDataFolder, "bob_live", "customer_address_region.RData"))
    SOAddressRegion <- GetSOAddressRegion(salesOrderAddress, customer_address_region)
    rm(salesOrderAddress)
    bobSalesOrderAddress <- left_join(bobSalesOrder, SOAddressRegion,
                                      by = c("fk_sales_order_address_shipping" = "id_sales_order_address"))
    packageDataBased <- left_join(packageDataBased, bobSalesOrderAddress,
                                  by = "order_nr")
    rm(SOAddressRegion, bobSalesOrder, bobSalesOrderAddress)
    gc()
    ##### Package Data #####
    packageNumberData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "oms_package", xMonth = monthsGoBack,
                                       startDate = startDate, 
                                       columnsList = c("id_package", "package_number"),
                                       keyField = "id_package")
    packageDataBased <- left_join(packageDataBased, packageNumberData,
                                  by = c("fk_package" = "id_package"))
    rm(packageNumberData)
    gc()
    ##### Shipment Provider Data #####
    packageDataBased <- left_join(packageDataBased, 
                                  oms_shipment_provider %>%
                                    select(id_shipment_provider, shipment_provider_name),
                                  by = c("fk_shipment_provider" = "id_shipment_provider"))
    
    
    ##### Supplier Address #####
    supplier_address <- GetXMonthData(ventureShort = ventureShort, dbName = "bob_live", tableName = "supplier_address", xMonth = "all",
                                      startDate = startDate, 
                                      columnsList = c("id_supplier_address", "fk_supplier", "address_type",
                                                      "city", "postcode"),
                                      keyField = "id_supplier_address")
    
    supplier_address_short <- supplier_address %>%
      filter(address_type == "warehouse") %>%
      mutate(postcode = as.numeric(postcode)) %>%
      filter(!is.na(postcode)) %>%
      select(fk_supplier, seller_postcode = postcode, seller_city = city)
    
    packageDataBased <- left_join(packageDataBased, supplier_address_short,
                                  by = c("fk_marketplace_merchant" = "fk_supplier"))
    
    rownames(oms_warehouse) <- oms_warehouse$id_warehouse
    packageDataBased %<>%
      mutate(origineName = ifelse(!is.na(fk_inventory), oms_warehouse[fk_mwh_warehouse, "name"],
                                  "seller_warehouse"))
    
    
    ##### Remove unneeded columns #####
    packageDataBased %<>%
      select(-c(id_sales_order_item, bob_id_sales_order_item, fk_sales_order,
                fk_sales_order_item_status, paid_price, sku, amount_paid, shipping_fee,
                shipping_surcharge, fk_mwh_warehouse,
                fk_marketplace_merchant, package_length, package_width,
                package_height, fk_package, id_package_dispatching,
                fk_shipment_provider, fk_sales_order_address_shipping))
    
    packageDataBased
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  packageDataBased
}
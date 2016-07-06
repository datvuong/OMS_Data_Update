args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])

tryCatch({
  
  source("3_Script/1_Code/00_init.R")
  source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
  source("3_Script/1_Code/03_Processing_Data/UpdateSOITimeStamp.R")
  source("3_Script/1_Code/03_Processing_Data/GetSixMonthData.R")
  source("3_Script/1_Code/03_Processing_Data/UpdateSOIBaseData.R")
  source(file.path("3_Script/1_Code/03_Processing_Data",ventureShort , paste0(ventureShort, "_GetSOAddressRegion.R")))
  source("3_Script/1_Code/03_Processing_Data/BuildPackageData.R")
  
  load(file.path(ventureDataFolder, "addressRegionData.RData"))
  load(file.path(ventureDataFolder, "oms_shipment_provider.RData"))
  load(file.path(ventureDataFolder, "skuData.RData"))
  load(file.path(ventureDataFolder, "supplier_address.RData"))
  load(file.path(ventureDataFolder, "oms_warehouse.RData"))
  
  reportName <- paste0(ventureShort, "_Build_Package_Base_Data")
  flog.appender(appender.tee(file.path("3_Script/2_Log",
                                       paste0(reportName,"_",dateReport,".csv"))),
                name = reportName)
  
  layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[Package_Base_Data]|~m'))
  flog.layout(layout, name=reportName)
  
  flog.info("Consolidate OMS Data", name = reportName)
  
  ##### History Timestamp Data #####
  soiHistoryData <- GetXMonthData(ventureShort = ventureShort, tableName = "ims_sales_order_item_status_history", xMonth = 6,
                                  keyField = "id_sales_order_item_status_history")
  soiTimestampData <- UpdateSOITimeStamp(soiHistoryData)
  soiTimestampData %<>%
    filter(!is.na(rts))
  rm(soiHistoryData)
  gc()
  ##### Sales Order Item Data #####
  salesOrderItem <- GetXMonthData(ventureShort = ventureShort, tableName = "ims_sales_order_item", xMonth = 12,
                                  columnsList = c("id_sales_order_item", "bob_id_sales_order_item", "fk_sales_order",
                                                  "fk_sales_order_item_status", "fk_marketplace_merchant", "unit_price",
                                                  "paid_price", "sku", "amount_paid", "shipping_fee", "shipping_discount_amount",
                                                  "shipping_surcharge", "fk_mwh_warehouse"),
                                  keyField = "id_sales_order_item")
  salesOrderItemBased <- inner_join(salesOrderItem, soiTimestampData,
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
  packageItemData <- GetXMonthData(ventureShort = ventureShort, tableName = "oms_package_item", xMonth = 12,
                                   columnsList = c("fk_package", "fk_sales_order_item", "fk_inventory"),
                                   keyField = "id_package_item")
  salesOrderItemBased <- left_join(salesOrderItemBased, packageItemData,
                                   by = c("id_sales_order_item" = "fk_sales_order_item"))
  rm(packageItemData)
  gc()
  ##### Package dispatching data #####
  packageDispatchingData <- GetXMonthData(ventureShort = ventureShort, tableName = "oms_package_dispatching", xMonth = 12,
                                          columnsList = c("id_package_dispatching", "fk_package", "fk_shipment_provider",
                                                          "tracking_number"),
                                          keyField = "id_package_dispatching")
  salesOrderItemBased <- left_join(salesOrderItemBased, packageDispatchingData,
                                   by = ("fk_package" = "fk_package"))
  rm(packageDispatchingData)
  gc()
  ##### SC Seller Data #####
  load(file.path(ventureDataFolder, "SCSellerData.RData"))
  salesOrderItemBased <- left_join(salesOrderItemBased, SCSellerData %>%
                                     filter(!is.na(src_id)),
                                   by = c("fk_marketplace_merchant" = "src_id"))
  rm(SCSellerData)
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
  packageDataBased <- packageData %>%
    filter(!duplicated(tracking_number))
  rm(packageData)
  gc()
  
  ##### Sales Order Data #####
  salesOrder <- GetXMonthData(ventureShort = ventureShort, tableName = "ims_sales_order", xMonth = 12,
                              columnsList = c("id_sales_order", "order_nr", "payment_method",
                                              "fk_sales_order_address_shipping"),
                              keyField = "id_sales_order")
  packageDataBased <- left_join(packageDataBased, salesOrder,
                                by = c("fk_sales_order" = "id_sales_order"))
  rm(salesOrder)
  gc()
  ##### Address Region Data #####
  salesOrderAddress <- GetXMonthData(ventureShort = ventureShort, tableName = "ims_sales_order_address", xMonth = "all",
                                     columnsList = c("id_sales_order_address", "ward", "city", "postcode",
                                                     "fk_customer_address_region"),
                                     keyField = "id_sales_order_address")
  SOAddressRegion <- GetSOAddressRegion(salesOrderAddress, addressRegionData)
  rm(salesOrderAddress)
  packageDataBased <- left_join(packageDataBased, SOAddressRegion,
                                by = c("fk_sales_order_address_shipping" = "id_sales_order_address"))
  rm(SOAddressRegion)
  gc()
  ##### Package Data #####
  packageNumberData <- GetXMonthData(ventureShort = ventureShort, tableName = "oms_package", xMonth = 12,
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

  supplier_address_short <- supplier_address %>%
    filter(address_type == "warehouse") %>%
    mutate(postcode = as.numeric(postcode)) %>%
    select(fk_supplier, seller_postcode = postcode, seller_city = city) %>%
    filter(!is.na(seller_postcode) & seller_postcode != "")
  
  packageDataBased <- left_join(packageDataBased, supplier_address_short,
                    by = c("fk_marketplace_merchant" = "fk_supplier"))
  
  rownames(oms_warehouse) <- oms_warehouse$id_warehouse
  packageDataBased %<>%
    mutate(origineName = ifelse(!is.na(fk_inventory), oms_warehouse[fk_mwh_warehouse, "name"],
                                "seller_warehouse"))
    
    
  ##### Remove unneeded columns #####
  packageDataBased %<>%
    select(-c(fk_marketplace_merchant, package_length, package_width,
              package_height, fk_package, id_package_dispatching,
              fk_shipment_provider, fk_sales_order_address_shipping))
  
  
  ##### Save final data #####
  save(packageDataBased, file = file.path(ventureDataFolder, "packageDataBased.RData"),
       compress = TRUE)
  flog.info("Done!!!", name = reportName)
  
}, error = function(err) {
  flog.error(paste(functionName, err, sep = " - "), name = reportName)
})
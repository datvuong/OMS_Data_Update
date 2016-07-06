args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/03_Processing_Data/UpdateSOITimeStamp.R")
source("3_Script/1_Code/03_Processing_Data/GetSixMonthData.R")
source("3_Script/1_Code/03_Processing_Data/UpdateSOIBaseData.R")
source(file.path("3_Script/1_Code/03_Processing_Data",ventureShort , paste0(ventureShort, "_GetSOAddressRegion.R")))
source("3_Script/1_Code/03_Processing_Data/BuildPackageData.R")

load(file.path(ventureDataFolder, "addressRegionData.RData"))
load(file.path(ventureDataFolder, "oms_shipment_provider.RData"))

reportName <- paste0(ventureShort, "_Build_Item_Base_Data")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[Item_Base_Data]|~m'))
flog.layout(layout, name=reportName)

salesOrderAddress <- GetXMonthData(ventureShort = ventureShort,dbName = "oms_live", tableName = "ims_sales_order_address", xMonth = "all",
                                   columnsList = c("id_sales_order_address", "ward", "city", "postcode",
                                                   "fk_customer_address_region"),
                                   keyField = "id_sales_order_address")
SOAddressRegion <- GetSOAddressRegion(salesOrderAddress, addressRegionData)
rm(salesOrderAddress)
gc()
flog.info("Consolidate OMS Data", name = reportName)

##### SOI Hisotry Timestamp #####
soiHistoryData <- GetXMonthData(ventureShort = ventureShort,dbName = "oms_live", tableName = "ims_sales_order_item_status_history", xMonth = 6,
                                keyField = "id_sales_order_item_status_history")
soiTimestampData <- UpdateSOITimeStamp(soiHistoryData)
soiTimestampData %<>%
  filter(!is.na(rts))
rm(soiHistoryData)
gc()

##### SOI Based Data ####
salesOrderItem <- GetXMonthData(ventureShort = ventureShort,dbName = "oms_live", tableName = "ims_sales_order_item", xMonth = 12,
                                columnsList = c("id_sales_order_item", "bob_id_sales_order_item", "fk_sales_order",
                                                "fk_sales_order_item_status", "fk_marketplace_merchant", "unit_price",
                                                "paid_price", "sku", "amount_paid",
                                                "shipping_fee", "shipping_discount_amount", "shipping_surcharge"),
                                keyField = "id_sales_order_item")
salesOrderItemBased <- inner_join(salesOrderItem, soiTimestampData,
                                  by = c("id_sales_order_item" = "fk_sales_order_item"),
                                  copy = TRUE)
rm(salesOrderItem, soiTimestampData)
gc()
##### SKU #####
load(file.path(ventureDataFolder, "skuData.RData"))
salesOrderItemBased <- left_join(salesOrderItemBased, skuData, 
                                 by = "sku", copy = TRUE)
rm(skuData)
gc()
salesOrderItemBased %<>%
  mutate(volumetricDimension = package_length * package_width * package_height)

##### Package Item #####
packageItemData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live", tableName = "oms_package_item", xMonth = 12,
                                 columnsList = c("fk_package", "fk_sales_order_item"),
                                 keyField = "id_package_item")
salesOrderItemBased <- left_join(salesOrderItemBased, packageItemData,
                                 by = c("id_sales_order_item" = "fk_sales_order_item"),
                                 copy = TRUE)
rm(packageItemData)
gc()
##### Package Dispatching #####
packageDispatchingData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live",tableName = "oms_package_dispatching", xMonth = 12,
                                        columnsList = c("id_package_dispatching", "fk_package", "fk_shipment_provider",
                                                        "tracking_number"),
                                        keyField = "id_package_dispatching")
salesOrderItemBased <- left_join(salesOrderItemBased, packageDispatchingData,
                                 by = ("fk_package" = "fk_package"), copy = TRUE)
rm(packageDispatchingData)
gc()
##### Seller Data ####
load(file.path(ventureDataFolder, "SCSellerData.RData"))
salesOrderItemBased <- left_join(salesOrderItemBased, SCSellerData %>%
                                   filter(!is.na(src_id)),
                                 by = c("fk_marketplace_merchant" = "src_id"), copy = TRUE)
rm(SCSellerData)
gc()

##### Sales Order Data ####
salesOrder <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live",tableName = "ims_sales_order", xMonth = 12,
                            columnsList = c("id_sales_order", "order_nr", "payment_method",
                                            "fk_sales_order_address_shipping"),
                            keyField = "id_sales_order")
salesOrderItemBased <- left_join(salesOrderItemBased, salesOrder,
                              by = c("fk_sales_order" = "id_sales_order"), copy = TRUE)
rm(salesOrder)
gc()
##### Package Number Data #####
packageNumberData <- GetXMonthData(ventureShort = ventureShort, dbName = "oms_live",tableName = "oms_package", xMonth = 12,
                                   columnsList = c("id_package", "package_number", "isdeleted"),
                                   keyField = "id_package")
salesOrderItemBased <- left_join(salesOrderItemBased, packageNumberData,
                              by = c("fk_package" = "id_package"), copy = TRUE)
rm(packageNumberData)
gc()

##### Shipment Provider #####
salesOrderItemBased <- left_join(salesOrderItemBased, 
                              oms_shipment_provider %>%
                                select(id_shipment_provider, shipment_provider_name),
                              by = c("fk_shipment_provider" = "id_shipment_provider"), copy = TRUE)

salesOrderItemBased %<>%
  select(-c(package_length, package_width,
            package_height, fk_package, id_package_dispatching,
            fk_shipment_provider, fk_sales_order_address_shipping))

save(salesOrderItemBased, file = file.path(ventureDataFolder, "salesOrderItemBased.RData"),
     compress = TRUE)
flog.info("Done!!!", name = reportName)

rm(list = ls())
suppressMessages({
  require(dplyr)
  require(tools)
  require(magrittr)
  require(methods)
  require(RMySQL)
  require(logging)
})
ims_sales_order_item
ims_sales_order_item_status_history
ims_sales_order_address
skuData.RData
oms_package_item # updated_at
oms_package_dispatching
ims_sales_order
oms_package
bob
sales_order
sales_order_address
supplier_address
customer_address_region

ventureShort <- as.character("ID")
dbName <- as.character("bob_live")
keyField <- as.character("id_sales_order")
tableName <- as.character("sales_order")
dataName <- as.character("sales_order")
timeField <- as.character("created_at")
extractLength <- as.numeric(3)
dateStart <- as.character("2016-03-01")
dateEnd <- as.character("2016-03-31")

startDate <- as.character("2016-03-31")
monthsGoBack <- as.integer(6)

database <- read.csv(file.path("3_Script/3_RData/TH/pmpdata", paste0(tableName, ".csv")), sep = ",", row.names = NULL)
eval(substitute(
  if ("created_at" %in% names(database)) {
    database %<>%
      mutate(created_at = as.POSIXct(created_at, format = "%Y-%m-%d %H:%M:%S"))
  },
  list()
))

eval(substitute(
  if ("updated_at" %in% names(database)) {
    database %<>%
      mutate(updated_at = as.POSIXct(updated_at, format = "%Y-%m-%d %H:%M:%S"))
  },
  list()
))

data

## batch get table data all

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/01_Loading/ExtractAllTableData.R")
source("3_Script/1_Code/02_Checking/CheckDataIntegrity.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)
layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', dbName, ".", tableName, ']|~m'))
flog.layout(layout, name=reportName)

load(file.path(ventureDataFolder, dbName, paste0(tableName, ".Rdata")))
database <- data
eval(substitute(
  data <- database #load(file.path(ventureDataFolder, dbName, paste0(tableName, ".Rdata")))
  #<- ExtractAllTableData(tableName = tableName, dbName = dbName,
#                               server = serverIP, username = user,
#                               password = password),
  , list(data = as.name(dataName))
))

eval(substitute(
  save(data, file = file.path(ventureDataFolder, dbName, paste0(dataName, ".RData"))),
  list(data = as.name(dataName))
))

CheckDataIntegrity(ventureShort = ventureShort, dbName = dbName, tableName = tableName, NULL,
                   paste0(tableName, "_summary"),
                   server = serverIP, username = user, password = password)

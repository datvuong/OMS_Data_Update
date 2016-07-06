args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
dateStart <- as.character(args[2])
dateEnd <- as.character(args[3])
dbName <- "oms_live"
tableName <- "ims_sales_order_address"
keyField <- "id_sales_order_address"
timeField <- "created_at"
dataName <- "ims_sales_order_address"
extractLength <- 3

if (length(args) == 8) {
  dateBegin <- as.character(args[8])
} else {
  dateBegin <- NULL
}

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/01_Loading/UpdateTableData.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', tableName, ']|~m'))
flog.layout(layout, name=reportName)


flog.info("Update Address Data", name = reportName)
dateBegin <- as.POSIXct(dateStart, format = "%Y-%m-%d")
lastDate <- as.POSIXct(dateEnd, format = "%Y-%m-%d")
while(dateBegin <= lastDate) {
  UpdateTableData(
    dataName = dataName, tableName = tableName, primaryKeyField = keyField,
    timeField = timeField,
    dbName = dbName, dateBegin = format(dateBegin, "%Y-%m-%d"), extractLength = extractLength,
    server = serverIP, username = user, password = password, 
    ventureDataFolder = ventureDataFolder)
  dateBegin <- dateBegin + ((extractLength + 1) * 3600 * 24)
}

flog.info("Done", name = reportName)



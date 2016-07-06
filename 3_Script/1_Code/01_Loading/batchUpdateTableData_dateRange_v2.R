args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
dbName <- as.character(args[2])
tableName <- as.character(args[3])
keyField <- as.character(args[4])
timeField <- as.character(args[5])
dataName <- as.character(args[6])
extractLength  <- as.numeric(args[7])

if (length(args) >= 8) {
  dateStart <- as.character(args[8])
} else {
  dateStart <- NULL
}

if (length(args) >= 9) {
  dateEnd <- as.character(args[9])
} else {
  dateEnd <- NULL
}

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/01_Loading/UpdateTableData.R")
source("3_Script/1_Code/02_Checking/CheckDataIntegrity.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', dbName, ".", tableName, ']|~m'))
flog.layout(layout, name=reportName)


flog.info(paste0("Update ", tableName, " Data"), name = reportName)
dateBegin <- as.POSIXct(dateStart, format = "%Y-%m-%d")
lastDate <- as.POSIXct(dateEnd, format = "%Y-%m-%d")
tried <- 0
while(dateBegin <= lastDate) {
  checking <- UpdateTableData(
    dataName = dataName, tableName = tableName, primaryKeyField = keyField,
    timeField = timeField,
    dbName = dbName, dateBegin = format(dateBegin, "%Y-%m-%d"), extractLength = extractLength,
    server = serverIP, username = user, password = password, 
    ventureDataFolder = ventureDataFolder)
  gc()
  dateBegin <- dateBegin + ((extractLength + 1) * 3600 * 24)
}

CheckDataIntegrity(ventureShort, dbName, tableName, timeField, paste0(dataName,"_summary"),
                   server = serverIP, username = user, password = password)

flog.info("Done", name = reportName)

rm(list = ls())
gc()
args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
dbName <- as.character(args[2])
tableName <- as.character(args[3])
dataName <- as.character(args[4])

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

eval(substitute(
  data <- ExtractAllTableData(tableName = tableName, dbName = dbName,
                              server = serverIP, username = user,
                          password = password),
  list(data = as.name(dataName))
))

eval(substitute(
  save(data, file = file.path(ventureDataFolder, dbName, paste0(dataName, ".RData"))),
  list(data = as.name(dataName))
))

CheckDataIntegrity(ventureShort = ventureShort, dbName = dbName, tableName = tableName, NULL,
                   paste0(tableName, "_summary"),
                   server = serverIP, username = user, password = password)

flog.info("Done", name = reportName)

rm(list = ls())
gc()
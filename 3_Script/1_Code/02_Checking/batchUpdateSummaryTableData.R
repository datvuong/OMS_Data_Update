args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
dbName <- as.character(args[2])
tableName <- as.character(args[3])
timeField <- as.character(args[4])
dataName <- as.character(args[5])

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/02_Checking/CheckDataIntegrity.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', tableName, ']|~m'))
flog.layout(layout, name=reportName)

CheckDataIntegrity(ventureShort, dbName, tableName, timeField, dataName)

flog.info("Done", name = reportName)



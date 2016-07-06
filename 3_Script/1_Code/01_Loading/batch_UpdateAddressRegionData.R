args <- commandArgs(trailingOnly = TRUE)
ventureShort <- as.character(args[1])
tableName <- "Address Region Data"

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/01_Loading/UpdateAddressRegionData.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', tableName, ']|~m'))
flog.layout(layout, name=reportName)


flog.info("Update Address Region Data", name = reportName)
UpdateAddressRegionData(server = serverIP,
                   username = user, password = password,
                   ventureDataFolder)

flog.info("Done", name = reportName)



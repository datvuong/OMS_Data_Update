args <- commandArgs(trailingOnly = TRUE)
ventureShort <- as.character(args[1])
tableName <- "SKU Data"

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/01_Loading/ExtractSKUData.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[', tableName, ']|~m'))
flog.layout(layout, name=reportName)

flog.info("Update Pacakge Data", name = reportName)
skuData <- ExtractSKUData(server = serverIP, username = user,
                          password = password)
tried <- 1
while (!is.data.frame(skuData) & tried <= 3) {
  flog.info(paste0("Try ", tried," times"), name = reportName)  
  skuData <- ExtractSKUData(server = serverIP, username = user,
                            password = password)
  tried <- tried + 1
}
if (is.data.frame(skuData)) {
  save(skuData, file = file.path(ventureDataFolder, "skuData.RData"),
       compress = TRUE)
  flog.info("SKU Data Update Success!!!", name = reportName)  
} else {
  flog.error("SKU Data Update Failed!!!", name = reportName)
}
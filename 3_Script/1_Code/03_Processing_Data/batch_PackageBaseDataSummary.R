source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/03_Processing_Data/BuildPackageBasedDataSummary.R")

args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
startDate <- as.character(args[2])
monthsGoBack <- as.integer(args[3])

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)
layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[PackageBasedData_Summary]|~m'))
flog.layout(layout, name=reportName)

tryCatch({
  
  savedFolder <- file.path("3_Script/3_RData", ventureShort)
  if (!dir.exists(savedFolder)) {
    dir.create(savedFolder)
  }
  
  startDate <- as.Date(startDate, format = "%Y-%m-%d")
  packageDataBased <- BuildPackageBasedDataSummary(venture = ventureShort, startDate = startDate, xMonth = monthsGoBack)
  
  save(packageDataBased, file = file.path(savedFolder, paste0("packageDataBased",".RData")),
       compress = TRUE)
  
}, error = function(err) {
  flog.error(paste("Main Error: ", err, sep = " - "), name = reportName)
})

flog.info("Done", name = reportName)
rm(list = ls())

args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])
startDate <- as.character(args[2])
monthsGoBack <- as.integer(args[3])

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/03_Processing_Data/BuildItemBasedData.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)
layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[salesOrderItemBased]|~m'))
flog.layout(layout, name=reportName)

tryCatch({
  
  savedFolder <- file.path("3_Script/3_RData", ventureShort, "salesOrderItemBased")
  if (!dir.exists(savedFolder)) {
    dir.create(savedFolder)
  }
  
  month <- format(as.POSIXct(startDate, format = "%Y-%m-%d"), "%Y%m")
  monthDataName <- paste0("salesOrderItemBased_", month)
  
  eval(substitute(
    {
      data <- BuildItemBasedData(venture = ventureShort, startDate = startDate, monthsGoBack = monthsGoBack)
    },
    list(data = as.name(monthDataName))
  ))
  
  eval(substitute(
    {
      if (is.data.frame(data)) {
        save(data, file = file.path(savedFolder, paste0(monthDataName,".RData")),
             compress = TRUE)
      }
    },
    list(data = as.name(monthDataName))
  ))
  
  
  
}, error = function(err) {
  flog.error(paste(functionName, err, sep = " - "), name = reportName)
})

flog.info("Done", name = reportName)
rm(list = ls())

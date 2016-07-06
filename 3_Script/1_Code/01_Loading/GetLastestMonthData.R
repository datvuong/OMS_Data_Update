GetLastestMonthData <- function(dataFilePath, dataName) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  functionName <- "GetLastestMonthData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    listDataFile <- list.files(dataFilePath)
    listDataFile <- listDataFile[grepl(paste0(dataName,"_.+\\.RData"), listDataFile)]
    
    if (length(listDataFile) >= 1) {
      lastMonthDataFile <- max(listDataFile)
      lastMonthData <- gsub("\\.RData", "", lastMonthDataFile)
      
      load(file.path(dataFilePath, lastMonthDataFile))
      
      eval(substitute(
        lastMonthdata,
        list(lastMonthdata = as.name(lastMonthData))
      ))
    } else {
      NULL
    }
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  output
}
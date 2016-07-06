GetXMonthData <- function(ventureShort, dbName, tableName, xMonth = 6, startDate = Sys.Date(),
                          columnsList = "All", keyField) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  functionName <- "GetXMonthData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, tableName, xMonth, "Months", sep = "-"), name = reportName)
  
  output <- tryCatch({
    dataPath <- file.path("3_Script/3_RData", ventureShort, dbName, tableName)
    if (is.numeric(xMonth)) {
      sixMonthList <- seq(startDate - xMonth * 30, by = "month", length = xMonth + 1)
      sixMonthList <- format(sixMonthList, "%Y%m")
      sixMonthList <- sort(sixMonthList, decreasing = TRUE)
      flog.info(paste("Function", functionName, tableName, paste(sixMonthList, collapse = "|"), "Months", sep = "-"), name = reportName)
      fileList <- paste0(tableName, "_", sixMonthList, ".RData")
    } else {
      fileList <- list.files(dataPath)
      fileList <- fileList[grepl("RData", fileList)]
    }
    
    data <- NULL
    for (iMonth in fileList) {
      tryCatch({
        if (file.exists(file.path(dataPath, iMonth))) {
          load(file.path(dataPath, iMonth))
          monthDataName <- gsub("\\.RData", "", iMonth)
          eval(substitute(
            if (columnsList != "All") {
              monthData <- monthData[c(columnsList, keyField)]
            },
            list(monthData = as.name(monthDataName))
          ))
          eval(substitute(
            if (is.null(data)) {
              data <- monthData
            } else {
              data <- rbind_list(data, monthData)
            },
            list(monthData = as.name(monthDataName))
          ))
        }
      }, error = function(err){
        flog.error(paste("Function", functionName, "Load Month Error: ", iMonth, err), name = reportName)
      })
    }
    
    eval(substitute(
      {
        data %<>%
          filter(!duplicated(keyField))
        if (columnsList != "All") {
          data <- data[columnsList]
        }
      },
      list(keyField = as.name(keyField))
    ))
    
    tbl_df(data)
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  output
}
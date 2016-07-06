BuildItemBasedDataSummary <- function(ventureShort, xMonth = 6, startDate = Sys.Date()) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  functionName <- "BuildItemBasedDataSummary"
  tableName <- "salesOrderItemBased"
  
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, tableName, xMonth, "Months", sep = "-"), name = reportName)
  
  
  output <- tryCatch({
    dataPath <- file.path("3_Script/3_RData", ventureShort, "salesOrderItemBased")
    if (is.numeric(xMonth)) {
      sixMonthList <- seq(startDate - xMonth * 30, by = "month", length = xMonth)
      sixMonthList <- format(sixMonthList, "%Y%m")
      flog.info(paste("Function", functionName, tableName, paste(sixMonthList, collapse = "|"), "Months", sep = "-"), name = reportName)
      fileList <- paste0(tableName, "_", sixMonthList, ".RData")
    } else {
      fileList <- list.files(dataPath)
      fileList <- fileList[grepl("RData", fileList)]
    }
    
    basedData <- NULL
    statusTimestampData <- NULL
    for (iMonth in fileList) {
     flog.info(paste("Function", functionName, "Month Process", iMonth, sep = "-"), name = reportName)
      tryCatch({
        if (file.exists(file.path(dataPath, iMonth))) {
          load(file.path(dataPath, iMonth))
          monthDataName <- gsub("\\.RData", "", iMonth)
          eval(substitute(
            {
              basedDataMonth <- monthData %>%
                select(-c(rts, shipped, cancelled, delivered, being_returned))
              statusTimestampDataMonth <- monthData %>%
                select(c(tracking_number, rts, shipped, cancelled, delivered, being_returned))
            },
            list(monthData = as.name(monthDataName))
          ))
          eval(substitute(
            {
              if (is.null(basedData)) {
                statusTimestampData <- statusTimestampDataMonth
                basedData <- basedDataMonth
              } else {
                statusTimestampData <- rbind_list(statusTimestampData, statusTimestampDataMonth)
                basedData <- rbind_list(basedData, basedDataMonth)
                basedData %<>% 
                  filter(!duplicated(tracking_number))
              }
            },
            list(monthData = as.name(monthDataName))
          ))
          
          rm(list = c(monthDataName, "basedDataMonth", "statusTimestampDataMonth"))
        }
      }, error = function(err){
       flog.error(paste("Function", functionName, "Load Month Error: ", iMonth, err), name = reportName)
      })
    }
    
    my_min <- function(x) {
      x_notNA <- x[!is.na(x)]
      if (length(x_notNA) == 0)
        x_notNA <- c(as.POSIXct(NA))
      return(min(x_notNA))
    }
    
    
    flog.info(paste("Function", functionName, "Consolidate Status Timestamp", sep = "-"), name = reportName)
    statusTimestampData %<>%
      group_by(tracking_number) %>% 
      summarize(rts = my_min(rts),
                shipped = my_min(shipped),
                cancelled = my_min(cancelled),
                delivered = my_min(delivered),
                being_returned = my_min(being_returned)) %>%
      ungroup() 
    
    flog.info(paste("Function", functionName, "Built Final Package Data", sep = "-"), name = reportName)
    basedData <- left_join(basedData, statusTimestampData,
                           by = "tracking_number")
    
    basedData
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  output
}
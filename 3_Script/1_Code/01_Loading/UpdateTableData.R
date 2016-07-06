UpdateTableData <- function(dataName, tableName, dbName,
                            primaryKeyField, timeField,
                            dateBegin = NULL, extractLength = 10,
                            server, username, password,
                            ventureDataFolder, batchSize = 50000) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(logging)
  })
  
  functionName <- "UpdateTableData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Update Table: ", tableName), name = reportName)
  
  output <- tryCatch({
    
    source("3_Script/1_Code/01_Loading/GetLastestMonthData.R")
    source("3_Script/1_Code/01_Loading/ExtractTableData.R")
    source("3_Script/1_Code/01_Loading/SaveDatabyMonth.R")
    
    dataFilePath <- file.path(ventureDataFolder, dbName, dataName)
    
    eval(substitute(
      if (dir.exists(dataFilePath)) {
        data <- GetLastestMonthData(dataFilePath, dataName)
      } else {
        dir.create(dataFilePath)
        data <- NULL
      },
      list(data = as.name(dataName))
    ))
    
    if (is.null(dateBegin)) {
      eval(substitute(
        if (is.null(dateBegin)) {
          if (is.null(data)) {
            dateBegin <- Sys.Date() - 380
          } else {
            dateBegin_1 <- Sys.Date() - extractLength
            dateBegin <- max(data$timeField)
            if (dateBegin > dateBegin_1) {
              dateBegin <- dateBegin_1
            }
          }
        },
        list(data = as.name(dataName),
             primaryKeyField = as.name(primaryKeyField),
             timeField = as.name(timeField))
      ))
      
      eval(substitute(
        flog.info(paste("Function", functionName, "Data rows before: ", nrow(data)), name = reportName),
        list(data = as.name(dataName),
             primaryKeyField = as.name(primaryKeyField))
      ))
    } else {
      month <- format(as.Date(dateBegin), "%Y%m")
      monthData <- paste0(dataName, "_", month)
      monthDataFile <- file.path(dataFilePath, paste0(monthData, ".RData"))
      if (file.exists(monthDataFile)) {
        load(monthDataFile)
        eval(substitute(
          data <- monthData,
          list(data = as.name(dataName),
               monthData = as.name(monthData))
        ))
      } else {
        eval(substitute(
          data <- NULL,
          list(data = as.name(dataName),
               monthData = as.name(monthData))
        ))
      }
    }
    
    if (is.na(dateBegin)) {
      dateBegin <- Sys.Date() - 380  
    }
    
    dateBeginShort <- as.Date(substr(dateBegin, 1, 10), format = "%Y-%m-%d", origin)
    dateEndShort <- min(dateBeginShort + extractLength, Sys.Date())
    dateEnd <- paste0(dateEndShort, " ", "23:59:59")
    
    if (dateBeginShort <= dateEndShort - 2) {
      flog.info(paste("Function", functionName, "Update Data", dateBegin, " => ", dateEnd), name = reportName)
      
      eval(substitute(
        {
          tried <- 0
          newData <- ExtractTableData(data = data, tableName = tableName, dbName = dbName,
                                   timeField = timeField, primaryKeyField = primaryKeyField,
                                   server = serverIP, username = user, 
                                   password = password, 
                                   dateBegin = dateBegin, dateEnd = dateEnd,
                                   batchSize = batchSize)
          while (!is.data.frame(newData) & tried <= 3) {
            flog.info(paste0("Try ", tried," times"), name = reportName)  
            newData <- ExtractTableData(data = data, tableName = tableName, dbName = dbName,
                                     timeField = timeField, primaryKeyField = primaryKeyField,
                                     server = serverIP, username = user, 
                                     password = password, 
                                     dateBegin = dateBegin, dateEnd = dateEnd,
                                     batchSize = batchSize)
            tried <- tried + 1
          }
          
          if (!is.data.frame(newData)) {
            return(FALSE)
          } else {
            data <- newData
            rm(newData)
            gc()
          }
        },
        list(data = as.name(dataName))
      ))
      
      
      eval(substitute(
        latestUpdatedTime <- as.Date(max(data$timeField)),
        list(data = as.name(dataName),
             primaryKeyField = as.name(primaryKeyField),
             timeField = as.name(timeField))
      ))
      
      eval(substitute(
        if (!is.na(match("created_at", names(data)))) {
          timeField <- "created_at"
        },
        list(data = as.name(dataName))
      ))
      
      eval(substitute(
        SaveDatabyMonth(ventureDataFolder = dataFilePath,
                        dataName = dataName, data = data,
                        timeField = timeField, primaryKeyField = primaryKeyField),
        list(data = as.name(dataName))
      ))
    } else {
      flog.info(paste("Function", functionName, "No Update - Latest Date Date :", dateBegin), name = reportName)
    }
    
    eval(substitute(
      flog.info(paste("Function", functionName, "Data rows after: ", nrow(data)), name = reportName),
      list(data = as.name(dataName),
           primaryKeyField = as.name(primaryKeyField))
    ))
    
    
    TRUE
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
    
    FALSE
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}
ExtractTableData <- function(data, tableName, dbName, timeField, primaryKeyField,
                             server, username, password, 
                             dateBegin, dateEnd, batchSize = 10000) {
  
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(RMySQL)
    require(logging)
  })
  
  functionName <- "ExtractTableData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Extract Table: ", tableName), name = reportName)
  
  output <- tryCatch({
    
    conn <- dbConnect(MySQL(), username = user,
                      password = password, host = server, port = 3306,
                      client.flag = 0)
    
    dbSendQuery(conn, "SET NAMES utf8")
    
    query <- 
      paste0("SELECT *
             FROM ", dbName,".", tableName,
             " WHERE (", timeField, " between '", dateBegin, "' and '", dateEnd,"')")
    
    rs <- dbSendQuery(conn, query)
    
    rowFetched <- 0
    rowCount <- 0
    newData <- NULL
    while (!dbHasCompleted(rs)) {
      temp <- dbFetch(rs, n = batchSize)
      rowFetched <- rowFetched + batchSize
      if (is.null(newData)) {
        newData <- temp
      } else {
        newData <- rbind_list(newData,temp)
      }
      if (rowCount < nrow(newData)) {
        rowCount <- nrow(newData)
        flog.info(paste("Function", functionName, "- Extracted: ", nrow(newData), " records"), name = reportName)
      } else {
        flog.info(paste("Function", functionName, "- Extracted: ", nrow(newData), " records"), name = reportName)
        flog.info(paste("Function", functionName, "- Lost Connnection"), name = reportName)
        return(FALSE)
      }
    }
    
    dbClearResult(rs)
    rm(temp)
    
    eval(substitute(
      if ("created_at" %in% names(newData)) {
        newData %<>%
          mutate(created_at = as.POSIXct(created_at, format = "%Y-%m-%d %H:%M:%S"))
      },
      list()
    ))
    
    eval(substitute(
      if ("updated_at" %in% names(newData)) {
        newData %<>%
          mutate(updated_at = as.POSIXct(updated_at, format = "%Y-%m-%d %H:%M:%S"))
      },
      list()
    ))
    
    data <- rbind_list(newData, data)
    
    eval(substitute(
      data %<>%
        filter(!duplicated(primaryKeyField)),
      list(timeField = as.name(timeField),
           primaryKeyField = as.name(primaryKeyField))
    ))
    
    data
    
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
    
    FALSE
  }, finally = {
    dbDisconnect(conn)
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}







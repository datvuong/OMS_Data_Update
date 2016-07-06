ExtractAllTableData <- function(tableName, dbName,
                             server, username, password, 
                             batchSize = 10000) {
  
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(RMySQL)
    require(logging)
  })
  
  functionName <- "ExtractAllTableData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Extract Table: ", tableName), name = reportName)
  
  output <- tryCatch({
    
    conn <- dbConnect(MySQL(), username = user,
                      password = password, host = server, port = 3306,
                      client.flag = 0)
    
    dbSendQuery(conn, "SET NAMES utf8")
    
    query <- 
      paste0("SELECT *
             FROM ", dbName,".", tableName)
    
    rs <- dbSendQuery(conn, query)
    data <- NULL
    rowFetched <- 0    
    while (!dbHasCompleted(rs)) {
      temp <- dbFetch(rs, n = batchSize)
      rowFetched <- rowFetched + batchSize
      if (is.null(data)) {
        data <- temp
      } else {
        data <- rbind(data,temp)
      }
    }
    
    dbClearResult(rs)
    rm(temp)
    
    eval(substitute(
      if ("created_at" %in% names(data)) {
        data %<>%
          mutate(created_at = as.POSIXct(created_at, format = "%Y-%m-%d %H:%M:%S"))
      },
      list()
    ))
    
    eval(substitute(
      if ("updated_at" %in% names(data)) {
        data %<>%
          mutate(updated_at = as.POSIXct(updated_at, format = "%Y-%m-%d %H:%M:%S"))
      },
      list()
    ))
    
    data
    
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    dbDisconnect(conn)
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}
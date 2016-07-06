QueryTableSummaryDataAll <- function(tableName, dbName, timeField,
                                  server, username, password) {
  
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(RMySQL)
    require(logging)
  })
  
  functionName <- "QueryTableSummaryData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Query Summary of Table: ", paste0(dbName, ".", tableName)), name = reportName)
  
  output <- tryCatch({
    
    conn <- dbConnect(MySQL(), username = username,
                      password = password, host = server, port = 3306,
                      client.flag = 0)
    
    dbSendQuery(conn, "SET NAMES utf8")
    
    if (timeField != "Missing") {
      query <- 
        paste0("SELECT 'All' month, COUNT(*) records, max(", timeField, ") latest_timestamp
               FROM ", dbName,".", tableName,
               " GROUP BY month;")
    } else {
      query <- 
        paste0("SELECT 'All' month, COUNT(*) records, NULL 'latest_timestamp'
               FROM ", dbName,".", tableName,
               " GROUP BY month;")
    }
    
    rs <- dbSendQuery(conn, query)
    
    rowFetched <- 0
    
    summaryData <- dbFetch(rs, n = -1)
    
    dbClearResult(rs)
    
    
    summaryData %<>%
      mutate(latest_timestamp = as.POSIXct(latest_timestamp, format = "%Y-%m-%d %H:%M:%S"))
    
    summaryData
    
  }, error = function(err) {
    flog.error(paste("Function", functionName, err, sep = " - "), name = reportName)
  }, finally = {
    dbDisconnect(conn)
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  output
}
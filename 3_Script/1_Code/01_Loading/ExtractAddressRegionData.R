ExtractAddressRegionData <- function(data,
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
  
  functionName <- "ExtractAddressRegionData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    conn <- dbConnect(MySQL(), username = user,
                      password = password, host = server, port = 3306,
                      client.flag = 0)
    
    dbSendQuery(conn, "SET NAMES utf8")
    
    query <- 
      paste0("SELECT *
         FROM oms_live.ims_customer_address_region addressRegion")
    
    flog.info(paste("Function", functionName, "Data rows before: ", nrow(data)), name = reportName)
    rs <- dbSendQuery(conn, query)
    
    rowFetched <- 0    
    while (!dbHasCompleted(rs)) {
      temp <- dbFetch(rs, n = batchSize)
      rowFetched <- rowFetched + batchSize
      if (is.null(data)) {
        data <- temp
      } else {
        data <- rbind(data, temp)
      }
    }
    
    flog.info(paste("Function", functionName, "Data rows after: ", nrow(data)), name = reportName)
    dbClearResult(rs)
    rm(temp)
    
    data
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    dbDisconnect(conn)
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}







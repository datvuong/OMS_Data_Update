ExtractSKUData <- function(server, username, password, 
                           batchSize = 100000) {
  
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(RMySQL)
    require(logging)
  })
  
  functionName <- "ExtractSKUData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    conn <- dbConnect(MySQL(), username = user,
                      password = password, host = server, port = 3306,
                      client.flag = 0)
    
    dbSendQuery(conn, "SET NAMES utf8")
    
    query <- 
      paste0("SELECT
                bobsku.sku,
                skuConfig.name product_name,
                skuConfig.package_length, 
                skuConfig.package_width, 
                skuConfig.package_height, 
                skuConfig.package_weight
            FROM bob_live.catalog_simple bobsku
            INNER JOIN bob_live.catalog_config skuConfig ON 
             bobsku.fk_catalog_config = skuConfig.id_catalog_config")
    
    rs <- dbSendQuery(conn, query)
    data <- NULL
    rowCount <- 0
    
    while (!dbHasCompleted(rs)) {
      temp <- dbFetch(rs, n = batchSize)
      if (is.null(data)) {
        data <- temp
      } else {
        data <- rbind(data,temp)
      }
      if (rowCount < nrow(data)) {
        rowCount <- nrow(data)
        flog.info(paste("Function", functionName, "- Extracted: ", nrow(data), " records"), name = reportName)
      } else {
        flog.info(paste("Function", functionName, "- Extracted: ", nrow(data), " records"), name = reportName)
        flog.error(paste("Function", functionName, "- Lost Connnection"), name = reportName)
        return(FALSE)
      }
    }
    
    cat("\r\n")
    print(nrow(data))
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







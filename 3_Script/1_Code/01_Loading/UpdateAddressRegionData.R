UpdateAddressRegionData <- function(server, username, password,
                                    ventureDataFolder) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(logging)
  })
  
  functionName <- "UpdateAddressRegionData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  
  output <- tryCatch({
    
    dataFilePath <- file.path(ventureDataFolder,"addressRegionData.RData")
    
    source("3_Script/1_Code/01_Loading/ExtractAddressRegionData.R")
    addressRegionData <- NULL
    
    addressRegionData <- ExtractAddressRegionData(addressRegionData,
                                        server = serverIP, username = user, 
                                        password = password, batchSize = 200000)
    
    save(addressRegionData, file = dataFilePath,
         compress = TRUE)
    
    TRUE
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}
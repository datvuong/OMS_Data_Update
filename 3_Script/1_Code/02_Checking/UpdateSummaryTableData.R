UpdateSummaryTableData <- function(dataName, tableName, dbName,
                                   timeField, server, username, password,
                                   ventureDataFolder) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(logging)
  })
  
  functionName <- "UpdateSummaryTableData"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Update Table: ", tableName), name = reportName)
  
  output <- tryCatch({
    
    source("3_Script/1_Code/02_Checking/QueryTableSummaryData.R")
    
    ventureDataFolder <- paste0(ventureDataFolder, "/SummaryTableData")
    if (!dir.exists(ventureDataFolder)) {
      dir.create(ventureDataFolder)
    }

    eval(substitute(
      data <- QueryTableSummaryData(data = data, tableName = tableName, dbName = dbName,
                               timeField = timeField, 
                               server = server, username = username, 
                               password = password),
      list(data = as.name(dataName))
    ))

    eval(substitute(
      return(data),
      list(data = as.name(dataName))
    ))
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
    
    FALSE
  }, finally = {
    flog.info(paste(functionName, "ended"), name = reportName)
  })
  
  output
}
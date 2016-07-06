CheckDataIntegrity <- function(ventureShort, dbName, tableName, timeField,
                               dataName, server, username , password) {
  
  source("3_Script/1_Code/02_Checking/QueryTableSummaryData.R")
  source("3_Script/1_Code/02_Checking/QueryTableSummaryDataAll.R")
  functionName <- "CheckDataIntegrity"
  
  dataFolder <- file.path(ventureDataFolder, dbName, tableName)
  if (is.null(timeField)) {
    
    load(paste0(dataFolder, ".RData"))
    eval(substitute(
      {
        if (any(names(monthData) == "created_at")) {
          timeField <- "created_at"
        } else if (any(names(monthData) == "updated_at")) {
          timeField <- "updated_at"
        } else {
          timeField <- "Missing"
        }
      },
      list(monthData = as.name(tableName))
    ))
    
    eval(substitute(
      {
        extracted_records <- nrow(monthData)
        if (timeField != "Missing") {
          extracted_latest_timestamp <- max(monthData$timeFieldName, na.rm = TRUE)
        } else {
          extracted_latest_timestamp <- NA
        }
      },
      list(monthData = as.name(tableName),
           timeFieldName = as.name(timeField))
    ))
    
    month <- "All"
    
    currentMonth <- data_frame(month, extracted_records, extracted_latest_timestamp)
    tableExtractedSummary <- currentMonth
    eval(substitute(
      rm(monthData),
      list(monthData = as.name(tableName))
    ))
    
    eval(substitute(
      data <- QueryTableSummaryDataAll(tableName = tableName, dbName = dbName,
                                       timeField = timeField,
                                       server = server, username = username, 
                                       password = password),
      list(data = as.name(dataName))
    ))
  } else {
    eval(substitute(
      data <- QueryTableSummaryData(tableName = tableName, dbName = dbName,
                                    timeField = timeField, 
                                    server = server, username = username, 
                                    password = password),
      list(data = as.name(dataName))
    ))
    
    tableExtractedSummary <- data_frame(month = character(),
                                        extracted_records = integer(),
                                        extracted_latest_timestamp = as.POSIXct(character()))
    for (iFile in list.files(dataFolder)) {
      tryCatch({
        monthData <- gsub(".RData", "", iFile)
        month <- gsub(tableName, "", monthData)
        month <- gsub("_", "", month)
        load(file.path(dataFolder, iFile))
        eval(substitute(
          {
            extracted_records <- nrow(monthData)
            extracted_latest_timestamp <- max(monthData$timeField)
          },
          list(monthData = as.name(monthData),
               timeField = as.name(timeField))
        ))
        currentMonth <- data_frame(month, extracted_records, extracted_latest_timestamp)
        tableExtractedSummary <- rbind_list(tableExtractedSummary, currentMonth)
        eval(substitute(
          rm(monthData),
          list(monthData = as.name(monthData))
        ))
      }, error = function(err){
        flog.error(paste("Function", functionName, "Load File Error: ", iFile), name = reportName)
      })
    }
  }

  
  eval(substitute(
    {
      data %<>% full_join(tableExtractedSummary,
                           by = "month")
      data %<>% mutate(integrityCheck = ifelse(extracted_records >= records, TRUE, FALSE))
    },
    list(data = as.name(dataName))
  ))
  
  eval(substitute(
    save(data, file = paste0(ventureDataFolder, "/SummaryTableData/", dbName, "/", dataName,".RData")),
    list(data = as.name(dataName))
  ))
}
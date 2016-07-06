SaveDatabyMonth <- function(ventureDataFolder,
                            dataName, data, timeField,
                            primaryKeyField) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  functionName <- "SaveDatabyMonth"
  flog.info(paste("Function", functionName, "started"), name = reportName)
  flog.info(paste("Function", functionName, "Folder Name:", ventureDataFolder), name = reportName)
  output <- tryCatch({
    
    eval(substitute(
      data %<>%
        mutate(dataMonth = format(timeField, "%Y%m")),
      list(timeField = as.name(timeField))
    ))
    
    listMonth <- unique(data$dataMonth)
    
    for (iMonth in listMonth) {
      monthDataName <- paste0(dataName, "_", iMonth)
      monthFileName <- paste0(monthDataName, ".RData")
      monthFilePath <- file.path(ventureDataFolder, monthFileName)
      if (file.exists(monthFilePath)) { 
        load(monthFilePath)
        eval(substitute(
          monthData <- rbind(
            data %>%
              filter(dataMonth == iMonth) %>%
              arrange(desc(timeField)) %>%
              filter(!duplicated(primaryKeyField)) %>%
              select(-c(dataMonth)),
            monthData) %>%
            filter(!duplicated(primaryKeyField)),
          list(monthData = as.name(monthDataName),
               timeField = as.name(timeField),
               primaryKeyField = as.name(primaryKeyField))
        ))
      } else {
        eval(substitute(
          monthData <- data %>%
            filter(dataMonth == iMonth) %>%
            filter(!duplicated(primaryKeyField)) %>%
            select(-c(dataMonth)),
          list(monthData = as.name(monthDataName),
               timeField = as.name(timeField),
               primaryKeyField = as.name(primaryKeyField))
        ))
      }
      
      eval(substitute(
        if (nrow(monthData) > 0) {
          save(monthData, file = monthFilePath,
               compress = TRUE)
        },
        list(monthData = as.name(monthDataName))
      ))
      
    }
    
  }, error = function(err) {
    flog.error(paste(functionName, err, sep = " - "), name = reportName)
  }, finally = {
    flog.info(paste("Function", functionName, "ended"), name = reportName)
  })
  
  output
}
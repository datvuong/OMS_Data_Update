source("3_Script/1_Code/00_init.R")

GetLogSummary <- function(ventureShort, date) {
  suppressMessages({
    require(dplyr)
    require(tools)
    require(magrittr)
    require(methods)
    require(futile.logger)
  })
  
  output <- tryCatch({
    
    logFile <- paste0("3_Script/2_Log/", ventureShort,"_OMS_Data_Update_", date, ".csv")
    rawLog <- read.csv(logFile, sep = "|",
                       col.names = c("runID", "report", "log", "timeStamp", "table", "message"))
    
    logSummary <- rawLog %>% group_by(runID, table) %>%
      mutate(timeStamp = as.POSIXct(gsub("(\\[|\\])","", timeStamp), format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Ho_Chi_Minh")) %>%
      mutate(rowsCountBefore = ifelse(grepl("Data rows before", message),
                                      as.numeric(gsub("[^0-9]", "", message)), 0)) %>%
      mutate(rowsCountAfter = ifelse(grepl("Data rows after", message),
                                     as.numeric(gsub("[^0-9]", "", message)), 0)) %>%
      mutate(dateData = ifelse(grepl("Update Data.+=>.+$", message),
                               gsub("[^0-9 -=>]", "", message),"")) %>%
      replace_na(list(rowsCountBefore = 0, rowsCountAfter = 0)) %>%
      group_by(runID, table) %>%
      summarize(withError = any(log == "[ERROR]"),
                isDone = any(message == "Done"),
                startTime = first(timeStamp),
                latestTime = last(timeStamp),
                runingTime = as.POSIXct(ifelse(isDone, last(timeStamp), Sys.time()),
                                        origin = origin)  - first(timeStamp),
                rowBefore = max(rowsCountBefore),
                rowAfter = max(rowsCountAfter),
                rowsExtracted = max(rowsCountAfter) - max(rowsCountBefore),
                dateData = max(dateData, na.rm = TRUE))
    
    logSummary
    
  }, error = function(err) {
  }, finally = {
  })
  
  output
}

IDLog <- GetLogSummary("ID", "20160112")
MYLog <- GetLogSummary("MY", "20160112")

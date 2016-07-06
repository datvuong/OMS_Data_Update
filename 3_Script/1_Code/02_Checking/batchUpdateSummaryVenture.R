args <- commandArgs(trailingOnly = TRUE)

ventureShort <- as.character(args[1])

source("3_Script/1_Code/00_init.R")
source("3_Script/1_Code/01_Loading/00_LoadingData_Init.R")
source("3_Script/1_Code/02_Checking/CheckDataIntegrity.R")

reportName <- paste0(ventureShort, "_OMS_Data_Update")
flog.appender(appender.tee(file.path("3_Script/2_Log",
                                     paste0(reportName,"_",dateReport,".csv"))),
              name = reportName)

layout <- layout.format(paste0(timeReport, '|', reportName, '|[~l]|[~t]|[CheckDataIntegrity]|~m'))
flog.layout(layout, name=reportName)

dbList <- c("bob_live", "oms_live", "screport")
ventureFolder <- file.path("/home/lazada/OMS_Data_Update/3_Script/3_RData", ventureShort)

for (iDB in dbList) {
  dbFolder <- file.path(ventureFolder, iDB)
  for (iFile in list.files(dbFolder)) {
    if (grepl("\\.RData", iFile)) {
      table <- gsub("\\.RData", "", iFile)
      CheckDataIntegrity(ventureShort = ventureShort, dbName = iDB, tableName = table, NULL,
                         paste0(table, "_summary"),
                         server = serverIP, username = user, password = password)
    } else if (dir.exists(file.path(file.path(dbFolder, iFile)))) {
      dataFolder <- file.path(file.path(dbFolder, iFile))
      if (length(list.files(dataFolder)) > 0) {
        sampleFile <- list.files(dataFolder)[1] 
        table <- gsub("\\.RData", "", sampleFile)
        load(file.path(dataFolder, sampleFile))
        eval(substitute(
          {
            if (any(names(monthData) == "created_at")) {
              timeField = "created_at"
            } else {
              timeField = "updated_at"
            }
          },
          list(monthData = as.name(table))
        ))
        rm(table)
        CheckDataIntegrity(ventureShort = ventureShort, dbName = iDB, tableName = iFile, timeField = timeField,
                           paste0(iFile, "_summary"),
                           server = serverIP, username = user, password = password)
      }
    }
  }
}

flog.info("Done", name = reportName)



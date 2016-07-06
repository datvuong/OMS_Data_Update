dateReport <- format(Sys.time(), "%Y%m%d")
timeReport <- format(Sys.time(), "%Y%m%d%H%M%S")
suppressMessages({
  options( java.parameters = "-Xmx4g" ) # Set heap memory for Java upto 4GB
  library(dplyr)
  library(tidyr)
  library(magrittr)
  library(methods)
  library(lubridate)
  library(futile.logger)
  library(XLConnect)
  library(RMySQL)
})


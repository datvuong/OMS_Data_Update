source(paste0("1_Input/",ventureShort,".txt"))

ventureDataFolder <- file.path("3_Script/3_RData", ventureShort)
if (!dir.exists(ventureDataFolder))
  dir.create(ventureDataFolder)

#!/bin/bash

#!/bin/bash

cd D:/Sinh_Projects/OMS_Data_Update/
trialVar=($(findstr "batchUpdateSummaryVenture.R --args $@"))
if [ ${#trialVar[@]} -le 1 ]
then 
  Rscript "3_Script/1_Code/02_Checking/batchUpdateSummaryVenture.R" "$@"
else
  echo ${#trialVar[@]}
  echo "Duplicated"
fi
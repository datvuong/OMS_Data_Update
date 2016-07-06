#!/bin/bash

#!/bin/bash

cd /home/lazada/OMS_Data_Update/
trialVar=($(pgrep -f "batchUpdateSummaryVenture.R --args $@"))
if [ ${#trialVar[@]} -le 1 ]
then 
  Rscript "3_Script/1_Code/02_Checking/batchUpdateSummaryVenture.R" "$@"
else
  echo ${#trialVar[@]}
  echo "Duplicated"
fi
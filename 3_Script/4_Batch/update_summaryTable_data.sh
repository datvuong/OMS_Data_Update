#!/bin/bash

cd /home/lazada/OMS_Data_Update/
array=("$@")
str=""
for arg in "${array[@]}"; do
    str=$str$arg" "
done
length=${#str}
length=$(($length-3))
str=$(echo $str| cut -c1-$length)
trialVar=($(pgrep -f "$str"))
if [ ${#trialVar[@]} -le 2 ]
then 
  Rscript "3_Script/1_Code/02_Checking/batchUpdateSummaryTableData.R" "$@"
else
  echo ${#trialVar[@]}
  echo "Duplicated"
fi


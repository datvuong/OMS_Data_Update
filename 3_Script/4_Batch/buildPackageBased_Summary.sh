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
trialVar=($(pgrep -f "batch_PackageBaseDataSummary.R $str"))
if [ ${#trialVar[@]} -le 1 ]
then 
  Rscript "3_Script/1_Code/03_Processing_Data/batch_PackageBaseDataSummary.R" "$@"
else
  echo ${#trialVar[@]}
  echo "Duplicated"
fi


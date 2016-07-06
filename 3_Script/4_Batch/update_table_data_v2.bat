#!/bin/bash

cd D:/Sinh_Projects/OMS_Data_Update/
array=("$@")
str=""
for arg in "${array[@]}"; do
    str=$str$arg" "
done
length=${#str}
length=$(($length-3))
str=$(echo $str| cut -c1-$length)
trialVar=($(findstr "$str"))
if [ ${#trialVar[@]} -le 2 ]
then 
  Rscript "3_Script/1_Code/01_Loading/batchUpdateTableData_dateRange_v2.R" "$@"
else
  echo ${#trialVar[@]}
  echo "Duplicated"
fi


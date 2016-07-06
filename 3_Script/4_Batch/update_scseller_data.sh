#!/bin/bash

cd /home/lazada/OMS_Data_Update/
Rscript "3_Script/1_Code/01_Loading/batch_LoadingSCSellerData.R" "$@"

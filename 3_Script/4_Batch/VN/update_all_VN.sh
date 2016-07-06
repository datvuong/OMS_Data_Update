#!/bin/bash

bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live oms_package id_package updated_at oms_package 5 >> ~/VN_oms_package.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live oms_package_item id_package_item updated_at oms_package_item 5 >> ~/VN_oms_package_item.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live oms_package_dispatching id_package_dispatching updated_at oms_package_dispatching 5 >> ~/VN_oms_package_dispatching 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live ims_sales_order id_sales_order created_at ims_sales_order 5 >> ~/VN_ims_sales_order.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live ims_sales_order_item id_sales_order_item updated_at ims_sales_order_item 5 >> ~/VN_ims_sales_order_item.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live ims_sales_order_item_status_history id_sales_order_item_status_history created_at ims_sales_order_item_status_history 5 >> ~/VN_ims_sales_order_item_status_history.log 2>&1 
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN screport transaction id_transaction created_at sc_transaction 15 >> ~/VN_sc_transaction.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN oms_live ims_sales_order_address id_sales_order_address updated_at ims_sales_order_address 5 >> ~/VN_ims_sales_order_address.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_table_data.sh" VN screport sales_order_item id_sales_order_item updated_at sc_sales_order_item 7 >> ~/VN_sc_sales_order_address.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_sku_data.sh" VN
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_scseller_data.sh" VN
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_address_region_data.sh" VN
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN oms_live oms_shipment_provider oms_shipment_provider >> ~/VN_oms_shipment_provider.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN bob_live supplier_address supplier_address >> ~/VN_supplier_address.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN oms_live ims_supplier_address ims_supplier_address >> ~/VN_ims_supplier_address.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN oms_live ims_supplier ims_supplier >> ~/VN_ims_supplier.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN oms_live oms_warehouse oms_warehouse >> ~/VN_oms_warehouse.log 2>&1
bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/get_all_table_data.sh" VN oms_live ims_supplier_address_region ims_supplier_address_region >> ~/VN_ims_supplier_address_region.log 2>&1
#bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_item_based.sh" VN >> ~/VN_item_base.log 2>&1
#bash "/home/sinh/Dropbox/lazada/OMS_Data_Update/3_Script/4_Batch/update_package_based.sh" VN >> ~/VN_package_base.log 2>&1
bash "/home/lazada/OMS_Data_Update/3_Script/4_Batch/bash_summary_table_data.sh" VN
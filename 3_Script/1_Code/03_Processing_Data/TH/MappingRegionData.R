source("3_Script/1_Code/00_init.R")
load("3_Script/3_RData/TH/addressRegionData.RData")

addressRegionData_rev <- addressRegionData %>%
  select(id_customer_address_region,
         fk_country,
         code,
         name,
         customer_address_region_type,
         fk_customer_address_region) %>%
  mutate(fk_customer_address_region = as.integer(as.character(fk_customer_address_region)))
addressRegionData_mapped <- addressRegionData_rev %>%
  filter(customer_address_region_type == 7) %>%
  select(id_customer_address_region,
         level_4_id_customer_address_region = id_customer_address_region,
         level_4_code = code,
         level_4_name = name,
         level_4_customer_address_region_type = customer_address_region_type,
         level_4_fk_customer_address_region = fk_customer_address_region)

addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                      addressRegionData_rev %>%
                                        select(level_3_id_customer_address_region = id_customer_address_region,
                                               level_3_code = code,
                                               level_3_name = name,
                                               level_3_customer_address_region_type = customer_address_region_type,
                                               level_3_fk_customer_address_region = fk_customer_address_region),
                                      by = c("level_4_fk_customer_address_region" = "level_3_id_customer_address_region"))

addressRegionData_mapped <- left_join(addressRegionData_mapped,
                                      addressRegionData_rev %>%
                                        select(level_2_id_customer_address_region = id_customer_address_region,
                                               level_2_code = code,
                                               level_2_name = name,
                                               level_2_customer_address_region_type = customer_address_region_type,
                                               level_2_fk_customer_address_region = fk_customer_address_region),
                                      by = c("level_3_fk_customer_address_region" = "level_2_id_customer_address_region"))

duplicatedCode <- addressRegionData_mapped$level_4_name[duplicated(addressRegionData_mapped$level_4_name)]
duplicatedPostalCode_data <- addressRegionData_mapped %>%
  filter(level_4_name %in% duplicatedCode) %>%
  select(level_2_id = level_3_fk_customer_address_region, level_2_name, level_2_code,
         level_3_id = level_4_fk_customer_address_region, level_3_name, level_3_code,
         level_4_id = level_4_id_customer_address_region, level_4_name, level_4_code) %>%
  arrange(level_4_name)

write.csv()
source("3_Script/1_Code/00_init.R")

load("3_Script/3_RData/addressRegionData.RData")

addressRegionData_rev <- addressRegionData %>%
  select(id_customer_address_region,
         fk_country,
         code,
         name,
         customer_address_region_type,
         fk_customer_address_region) %>%
  mutate(fk_customer_address_region = as.integer(as.character(fk_customer_address_region)))
addressRegionData_mapped <- addressRegionData_rev %>%
  filter(customer_address_region_type == 4) %>%
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

addressRegionData_mapped

##### RPX Mapping #####
wb <- loadWorkbook("1_Input/RPX/02_Ratecards/RPX Master posatl Code-Lazada 8-9-15.xlsx")  
RPXRateCard <- readWorksheet(object = wb, sheet = 1)
RPXRateCardRev <- RPXRateCard %>%
  select(PROPINSI, KOTA.KABUPATEN, KECAMATAN, RATE.KG) %>%
  mutate(PROPINSI = gsub("[^a-zA-Z0-9]", "", toupper(PROPINSI))) %>%
  mutate(KOTA.KABUPATEN = gsub("[^a-zA-Z0-9]", "", toupper(KOTA.KABUPATEN))) %>%
  mutate(KECAMATAN = gsub("[^a-zA-Z0-9]", "", toupper(KECAMATAN))) %>%
  mutate(mappingCode = paste0(PROPINSI, KOTA.KABUPATEN, KECAMATAN)) %>%
  arrange(mappingCode, desc(RATE.KG)) %>%
  filter(!duplicated(mappingCode)) %>%
  select(-c(mappingCode))

addressRegionData_mappedRPX <- addressRegionData_mapped %>%
  mutate(level_2_name = gsub("[^a-zA-Z0-9]", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("[^a-zA-Z0-9]", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("[^a-zA-Z0-9]", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("^(KAB|KOTA)", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("^(KAB|KOTA)", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("(KAB|KOTA)$", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("(KAB|KOTA)$", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("(KAB|KOTA)$", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("ANYERANYAR", "ANYAR", level_4_name)) %>%
  mutate(level_4_name = gsub("KOTOVIITUJUH", "KOTOVII", level_4_name)) %>%
  mutate(level_4_name = gsub("POSOUTARA", "POSO", level_4_name))
  
mappedRateCard <- left_join(addressRegionData_mappedRPX, 
                            RPXRateCardRev,
                            by = c("level_2_name" = "PROPINSI",
                                   "level_3_name" = "KOTA.KABUPATEN",
                                   "level_4_name" = "KECAMATAN"))

finalMappedRateCard <- left_join(addressRegionData_mapped, 
                                 mappedRateCard %>%
                                   select(level_4_id_customer_address_region,
                                          RPX_Rate=RATE.KG),
                                 by = "level_4_id_customer_address_region")

##### LEX Mapping #####
wb <- loadWorkbook("1_Input/LEX/02_Ratecards/LEX Rate Card-Final to Lzd.xlsx")  
LEXRateCard <- readWorksheet(object = wb, sheet = 1)

LEXRateCardRev <- LEXRateCard %>%
  select(Banten, City_name, District_name,
         Initial.1st.Kg, Next.Kg, Insurance.Charge, COD.Fee) %>%
  mutate(Banten = gsub("[^a-zA-Z0-9]", "", toupper(Banten))) %>%
  mutate(City_name = gsub("[^a-zA-Z0-9]", "", toupper(City_name))) %>%
  mutate(District_name = gsub("[^a-zA-Z0-9]", "", toupper(District_name))) %>%
  mutate(Banten = gsub("^(KAB|KOTA)", "", toupper(Banten))) %>%
  mutate(City_name = gsub("^(KAB|KOTA)", "", toupper(City_name))) %>%
  mutate(District_name = gsub("^(KAB|KOTA)", "", toupper(District_name))) %>%
  mutate(Banten = gsub("(KAB|KOTA)$", "", toupper(Banten))) %>%
  mutate(City_name = gsub("(KAB|KOTA)$", "", toupper(City_name))) %>%
  mutate(District_name = gsub("(KAB|KOTA)$", "", toupper(District_name)))
LEXRateCardRev <- filter(rateCardRev, !duplicated(rateCardRev))

addressRegionData_mappedLEX <- addressRegionData_mapped %>%
  mutate(level_2_name = gsub("[^a-zA-Z0-9]", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("[^a-zA-Z0-9]", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("[^a-zA-Z0-9]", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("^(KAB|KOTA)", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("^(KAB|KOTA)", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("(KAB|KOTA)$", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("(KAB|KOTA)$", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("(KAB|KOTA)$", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("GROGOLPETAMBURAN", "GROGOL", level_4_name)) %>%
  mutate(level_4_name = gsub("CIMEUNYAN", "CIMENYAN", level_4_name)) %>%
  mutate(level_4_name = gsub("MAKASAR", "MAKASSAR", level_4_name)) %>% 
  mutate(level_4_name = gsub("BABAKANCIPARAY", "CIPARAY", level_4_name)) %>%
  mutate(level_4_name = gsub("BUAHBATUMARGACINTA", "MARGACINTA", level_4_name)) %>%
  mutate(level_4_name = gsub("SUKARAMI", "SUKARAME", level_4_name)) %>%
  mutate(level_4_name = gsub("ANTAPANICICADAS", "CICADAS", level_4_name)) %>%
  mutate(level_4_name = gsub("BUNGUSTELUKKABUNG", "BUNGUSTELUKUNG", level_4_name)) %>%
  mutate(level_4_name = gsub("MENGWI", "MENGUWI", level_4_name)) %>%
  mutate(level_4_name = gsub("KLAPANUNGGALKELAPANUNGGAL", "KLAPANUNGGAL", level_4_name)) %>%
  mutate(level_4_name = gsub("BUNGUSTELUKUNG", "BUNGUSTELUKKABUNG", level_4_name)) %>%
  mutate(level_4_name = gsub("BAKAUHENI", "BAKAUHEUNI", level_4_name))

mappedRateCard <- left_join(addressRegionData_mappedLEX, 
                            LEXRateCardRev,
                            by = c("level_2_name" = "Banten",
                                   "level_3_name" = "City_name",
                                   "level_4_name" = "District_name"))

mappedRateCard <- mappedRateCard %>% filter(!duplicated(level_4_id_customer_address_region))

finalMappedRateCardLEX <- left_join(finalMappedRateCard,
                                 mappedRateCard %>%
                                   select(level_4_id_customer_address_region,
                                          LEX_Rate=Initial.1st.Kg),
                                 by = "level_4_id_customer_address_region")

##### TIKI ####
wb <- loadWorkbook("1_Input/Tiki/02_Ratecards/TIKI Tarif Domestik 2014 (Corporate).xlsx")  
TIKIRateCard <- readWorksheet(object = wb, sheet = "Sheet1")

TIKIRateCardRev <- TIKIRateCard %>%
  select(NAMA..KOTA, RATE.KG = Reg..KGP) %>%
  mutate(NAMA..KOTA = gsub("[^a-zA-Z0-9]", "", toupper(NAMA..KOTA))) %>%
  mutate(NAMA..KOTA = gsub("^(KAB|KOTA)", "", toupper(NAMA..KOTA))) %>%
  mutate(NAMA..KOTA = gsub("(KAB|KOTA)$", "", toupper(NAMA..KOTA)))

addressRegionData_mappedTIKI <- addressRegionData_mapped %>%
  mutate(level_2_name = gsub("[^a-zA-Z0-9]", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("[^a-zA-Z0-9]", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("[^a-zA-Z0-9]", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("^(KAB|KOTA)", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("^(KAB|KOTA)", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("(KAB|KOTA)$", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("(KAB|KOTA)$", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("(KAB|KOTA)$", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("SIBOLANGIT", "SIKEBENSIBOLANGIT", level_4_name)) %>%
  mutate(mappingKey = paste0(level_4_name, level_3_name))
  
mappedRateCard <- left_join(addressRegionData_mappedTIKI, 
                            TIKIRateCardRev,
                            by = c("mappingKey" = "NAMA..KOTA"))

finalMappedRateCardTIKI <- left_join(finalMappedRateCardLEX,
                                    mappedRateCard %>%
                                      select(level_4_id_customer_address_region,
                                             TIKIRate=RATE.KG),
                                    by = "level_4_id_customer_address_region")

##### FL #####
wb <- loadWorkbook("1_Input/FL/02_Ratecards/FL_ratecard.xlsx")  
FLRateCard <- readWorksheet(object = wb, sheet = 1, colTypes = c(XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING,
                                                               XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING,
                                                               XLC$DATA_TYPE.NUMERIC, XLC$DATA_TYPE.NUMERIC, XLC$DATA_TYPE.NUMERIC,
                                                               XLC$DATA_TYPE.NUMERIC, XLC$DATA_TYPE.STRING))

FLRateCardRev <- FLRateCard %>%
  select(Region_name, City_name, District_name, first_1kg, add_1kg, Insurance_Charge, COD_Fee) %>%
  mutate(Region_name = gsub("[^a-zA-Z0-9]", "", toupper(Region_name))) %>%
  mutate(City_name = gsub("[^a-zA-Z0-9]", "", toupper(City_name))) %>%
  mutate(District_name = gsub("[^a-zA-Z0-9]", "", toupper(District_name))) %>%
  mutate(Region_name = gsub("^(KAB|KOTA)", "", toupper(Region_name))) %>%
  mutate(City_name = gsub("^(KAB|KOTA)", "", toupper(City_name))) %>%
  mutate(District_name = gsub("^(KAB|KOTA)", "", toupper(District_name))) %>%
  mutate(Region_name = gsub("(KAB|KOTA)$", "", toupper(Region_name))) %>%
  mutate(City_name = gsub("(KAB|KOTA)$", "", toupper(City_name))) %>%
  mutate(District_name = gsub("(KAB|KOTA)$", "", toupper(District_name))) %>%
  mutate(mappingCode = paste0(Region_name, City_name, District_name)) %>%
  arrange(mappingCode, desc(first_1kg)) %>%
  filter(!duplicated(mappingCode)) %>%
  select(-c(mappingCode))

addressRegionData_mappedFL <- addressRegionData_mapped %>%
  mutate(level_2_name = gsub("[^a-zA-Z0-9]", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("[^a-zA-Z0-9]", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("[^a-zA-Z0-9]", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("^(KAB|KOTA)", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("^(KAB|KOTA)", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_2_name = gsub("(KAB|KOTA)$", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("(KAB|KOTA)$", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("^(KAB|KOTA)", "", toupper(level_4_name))) %>%
  mutate(level_4_name = gsub("(KAB|KOTA)$", "", toupper(level_4_name)))

mappedRateCard <- left_join(addressRegionData_mappedFL, 
                            FLRateCardRev,
                            by = c("level_2_name" = "Region_name",
                                   "level_3_name" = "City_name",
                                   "level_4_name" = "District_name"))

finalMappedRateCardFL <- left_join(finalMappedRateCardTIKI,
                                   mappedRateCard %>%
                                       select(level_4_id_customer_address_region,
                                              FLRate=first_1kg),
                                     by = "level_4_id_customer_address_region")

##### PANDU #####

wb <- loadWorkbook("1_Input/Pandu/02_Ratecards/PANDU_ratecard.xls")  
PanduRateCard <- readWorksheet(object = wb, sheet = 1, colTypes = c(XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING,
                                                               XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.STRING,
                                                               XLC$DATA_TYPE.STRING, XLC$DATA_TYPE.NUMERIC, XLC$DATA_TYPE.NUMERIC,
                                                               XLC$DATA_TYPE.NUMERIC))

PanduRateCardRev <- PanduRateCard %>%
  select(Origin,	Destination,	Destination_Desc, Treshold,	Price,	Lead_Time) %>%
  mutate(Destination_Desc = gsub("[^a-zA-Z0-9]", "", toupper(Destination_Desc))) %>%
  mutate(mappingCode = Destination_Desc) %>%
  arrange(mappingCode, desc(Price)) %>%
  filter(!duplicated(mappingCode)) %>%
  select(-c(mappingCode))

addressRegionData_mappedPandu <- addressRegionData_mapped %>%
  mutate(level_2_name = gsub("[^a-zA-Z0-9]", "", toupper(level_2_name))) %>%
  mutate(level_3_name = gsub("[^a-zA-Z0-9]", "", toupper(level_3_name))) %>%
  mutate(level_4_name = gsub("[^a-zA-Z0-9]", "", toupper(level_4_name)))


mappedRateCard <- left_join(addressRegionData_mappedPandu, 
                            PanduRateCardRev,
                            by = c("level_3_name" = "Destination_Desc"))

finalMappedRateCardPandu <- left_join(finalMappedRateCardFL,
                                   mappedRateCard %>%
                                     select(level_4_id_customer_address_region,
                                            PanduRate=Price),
                                   by = "level_4_id_customer_address_region")

write.csv(finalMappedRateCardPandu, "2_Output/region_mapped_ratecard.csv",
          row.names = FALSE)

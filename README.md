# epa-justice

## Background
Accessing US Census data and CDC health data via API to support demographic data summaries in SNAP's [Northern Climate Reports](https://northernclimatereports.org/).

This repo includes functions and lookup tables to support the EPA-Justice-HIA project which pulls demographic data from the US Census and CDC datasets. The functions here will take Alaska community IDs (from [this collection](https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/point/alaska_point_locations.csv)) as their input, cross-reference these communities to their respective geographies using the most recent `NCRPlaces_Census_{MMDDYYYY}.csv` table, and compute data items relevant to the EPA-Justice-HIA project. These functions and/or the resulting tables will be incorporated into Northern Climate Reports.

Some places have a one-to-many relationship with census geographies. In these cases, when results are available for every geographic unit, value are aggregated according to the guidance [here](https://www.cdc.gov/places/faqs/using-data/index.html). If results are unavailable for any geographic unit, values are not aggregated and and instead replaced with NA.

This repo accesses US Census datasets via their survey-specific API endpoints ([ACS 5-year](https://www.census.gov/data/developers/data-sets/acs-5year.html) and [DHC](https://www.census.gov/data/developers/data-sets/decennial-census.html)), the CDC [PLACES](https://www.cdc.gov/places/index.html) datasets via their geography-specific API endpoints (for [county](https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-County-Data-20/swc5-untb/about_data), [place](https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-Place-Data-202/eav7-hnsx/about_data), [tract](https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-Census-Tract-D/cwsq-ngmh/about_data), and [zip code](https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-ZCTA-Data-2023/qnzd-25i4/about_data)), and the CDC [SDOH](https://www.cdc.gov/places/social-determinants-of-health-and-places-data/index.html) datasets via their geography-specific API endpoints (for [county](https://data.cdc.gov/500-Cities-Places/SDOH-Measures-for-County-ACS-2017-2021/i6u4-y3g4/about_data), [place](https://data.cdc.gov/500-Cities-Places/SDOH-Measures-for-Place-ACS-2017-2021/edkk-ze78/about_data), [tract](https://data.cdc.gov/500-Cities-Places/SDOH-Measures-for-Census-Tract-ACS-2017-2021/e539-uadk/about_data), and [zip code](https://data.cdc.gov/500-Cities-Places/SDOH-Measures-for-ZCTA-ACS-2017-2021/bumh-rgsq/about_data)). Note that zip code geographies are not currently used in this project, but the codebase here supports using zip codes if desired.

## Processing instructions

- Activate a `conda` environment that includes `pandas` (>2.0) and `numpy`.
- Use the `fetch_data_and_export.ipynb` notebook to run the processing pipeline. (This notebook also contains some tests of individual functions used for review during development.)
- View the `data_to_export.csv` results.

This repo also contains additional processing tools that were used for adding new places to the [GVV repo](https://github.com/ua-snap/geospatial-vector-veracity) that were required for this specific project (`update_NCR_points.ipynb`, `add_to_NCR.csv`,   `alaska_point_locations.csv`, and `add_point_location.py`). These shouldn't need to be run again, but are saved here for now in case the project scope changes and more places need to be added.

## Data Dictionary

The data variables below are pulled for each geography. The `short name` column is the abbreviated name used in the exported `data_to_export.csv`. All other column names should be self explanatory.

### Census DHC Year 2020 - Raw data

| Variable ID | long name | short name |
| -------- | ------- | ------ |
| P12_001N | !!Total: | total_population |
| P12_002N | !!Total:!!Male:: SEX BY AGE FOR SELECTED AGE CATEGORIES | total_male |
| P12_026N | !!Total:!!Female:: SEX BY AGE FOR SELECTED AGE CATEGORIES | total_female |
| P12_003N | !!Total:!!Male:!!Under 5 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_under_5 |
| P12_004N | !!Total:!!Male:!!5 to 9 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_5_to_9 |
| P12_005N | !!Total:!!Male:!!10 to 14 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_10_to_14 |
| P12_006N | !!Total:!!Male:!!15 to 17 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_15_to_17 |
| P12_020N | !!Total:!!Male:!!65 and 66 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_65_to_66 |
| P12_021N | !!Total:!!Male:!!67 to 69 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_67_to_69 |
| P12_022N | !!Total:!!Male:!!70 to 74 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_70_to_74 |
| P12_023N | !!Total:!!Male:!!75 to 79 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_75_to_79 |
| P12_024N | !!Total:!!Male:!!80 to 84 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_80_to_84 |
| P12_025N | !!Total:!!Male:!!85 years and over: SEX BY AGE FOR SELECTED AGE CATEGORIES | m_85_plus |
| P12_027N | !!Total:!!Female:!!Under 5 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_under_5 |
| P12_028N | !!Total:!!Female:!!5 to 9 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_5_to_9 |
| P12_029N | !!Total:!!Female:!!10 to 14 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_10_to_14 |
| P12_030N | !!Total:!!Female:!!15 to 17 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_15_to_17 |
| P12_044N | !!Total:!!Female:!!65 and 66 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_65_to_66 |
| P12_045N | !!Total:!!Female:!!67 to 69 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_67_to_69 |
| P12_046N | !!Total:!!Female:!!70 to 74 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_70_to_74 |
| P12_047N | !!Total:!!Female:!!75 to 79 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_75_to_79 |
| P12_048N | !!Total:!!Female:!!80 to 84 years: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_80_to_84 |
| P12_049N | !!Total:!!Female:!!85 years and over: SEX BY AGE FOR SELECTED AGE CATEGORIES | f_85_plus |
| P9_001N | !!Total | total_p9 |
| P9_002N | !!Total:!!Hispanic or Latino | hispanic_latino |
| P9_005N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone | white |
| P9_006N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone | african_american | 
| P9_007N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!American Indian and Alaska Native alone | amer_indian_ak_native |
| P9_008N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone | asian |
| P9_009N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone | hawaiian_pacislander |
| P9_010N | !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone | other |
| P9_011N | !!Total:!!Not Hispanic or Latino:!!Population of two or more races: | multi |


### Census DHC Year 2020 - Calculated from raw data
| Variable ID | long name | short name
| -------- | ------- | ------ |
| NA | Percentage of population 65 and older| pct_65_plus |
| NA | Percentage of population under age 18 | pct_under_18 |
| NA | Percentage of population under age 5 | pct_under_5 |
| NA | Percentage of population Hispanic or Latino | pct_hispanic_latino |
| NA | Percentage of population White | pct_white |
| NA | Percentage of population African American | pct_african_american |
| NA | Percentage of population American Indian or Alaska Native | pct_amer_indian_ak_native |
| NA | Percentage of population Asian | pct_asian |
| NA | Percentage of population Native Hawaiian and Pacific Islander | pct_hawaiian_pacislander |
| NA | Percentage of population Other Race | pct_other |
| NA | Percentage of population Two or More Races | pct_multi |


### Census ACS 5-year (2018-2022)
| Variable ID | long name | short name
| -------- | ------- | ------ |
| S1810_C03_001E | Percent with a disability!!Estimate!!Total civilian noninstitutionalized population | pct_w_disability |
| S1810_C03_001M | Margin of Error!!Percent with a disability!!Total civilian noninstitutionalized population | moe_pct_w_disability |
| S2701_C03_001E | Estimate!!Percent Insured!!Civilian noninstitutionalized population | pct_insured |
| S2701_C03_001M | Margin of Error!!Percent Insured!!Civilian noninstitutionalized population | moe_pct_insured |
| S2701_C05_001E | Estimate!!Percent Uninsured!!Civilian noninstitutionalized population | pct_uninsured |
| S2701_C05_001M | Margin of Error!!Percent Uninsured!!Civilian noninstitutionalized population	| moe_pct_uninsured |


### CDC PLACES Year 2024
| Variable ID | long name | short name | data value type |
| -------- | ------- | ------ | ------ |
| CASTHMA | Current asthma among adults aged >=18 years | pct_asthma | crude prevalence |               
| COPD | Chronic obstructive pulmonary disease among adults aged >=18 years | pct_copd | crude prevalence |             
| CHD | Coronary heart disease among adults aged >=18 years | pct_hd | crude prevalence | 
| STROKE | Stroke among adults aged >=18 years | pct_stroke | crude prevalence |            
| DIABETES | Diagnosed diabetes among adults aged >=18 years | pct_diabetes | crude prevalence |    
| MHLTH | Frequent mental distress among adults aged >=18 years | pct_mh | crude prevalence |
| FOODSTAMP | Received food stamps in the past 12 months among adults aged >=18 years | pct_foodstamps | crude prevalence |
| EMOTIONSPT | Lack of social and emotional support among adults aged >=18 years | pct_emospt | crude prevalence |


### CDC SDOH (2017-2021)
| Variable ID | long name | short name |
| -------- | ------- | ------ |
REMNRTY | Persons of racial or ethnic minority status | pct_minority |
NOHSDP | No high school diploma among adults aged 25 years or older | pct_no_hsdiploma |
POV150 | Persons living below 150% of the poverty level | pct_below_150pov |
BROAD | No broadband internet subscription among households | pct_no_bband |
CROWD | Crowding among housing units | pct_crowding
HCOST | Housing cost burden among households | pct_hcost
SNGPNT | Single-parent households | pct_single_parent
UNEMP | Unemployment among people 16 years or older in the labor force | pct_unemployed
                

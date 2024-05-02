#census API key
census_ = "839fc96162a9e16e7896434e7592eccaf7938706"

#CDC app token
cdc_ = "aBj1zOdJs530ivzSUlyOHkLaf"

var_dict = {
    "dhc": {
        "url": "https://api.census.gov/data/2020/dec/dhc",
        "vars": {
            # total
            "P12_001N": {
                "long_name": "!!Total:", # total from P12 table only: SEX BY AGE FOR SELECTED AGE CATEGORIES
                "short_name": "total_population",
            },
            # male and female totals
            "P12_002N": {
                "long_name": "!!Total:!!Male:: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "total_male",
            },
            "P12_026N": {
                "long_name": "!!Total:!!Female:: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "total_female",
            },
            # male by age category
            "P12_003N": {
                "long_name": "!!Total:!!Male:!!Under 5 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_under_5",
            },
            "P12_004N": {
                "long_name": "!!Total:!!Male:!!5 to 9 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_5_to_9",
            },
            "P12_005N": {
                "long_name": "!!Total:!!Male:!!10 to 14 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_10_to_14",
            },
            "P12_006N": {
                "long_name": "!!Total:!!Male:!!15 to 17 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_15_to_17",
            },
            "P12_020N": {
                "long_name": "!!Total:!!Male:!!65 and 66 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_65_to_66",
            },
            "P12_021N": {
                "long_name": "!!Total:!!Male:!!67 to 69 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_67_to_69",
            },
            "P12_022N": {
                "long_name": "!!Total:!!Male:!!70 to 74 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_70_to_74",
            },
            "P12_023N": {
                "long_name": "!!Total:!!Male:!!75 to 79 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_75_to_79",
            },
            "P12_024N": {
                "long_name": "!!Total:!!Male:!!80 to 84 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_80_to_84",
            },
            "P12_025N": {
                "long_name": "!!Total:!!Male:!!85 years and over: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "m_85_plus",
            },
            # female by age category
            "P12_027N": {
                "long_name": "!!Total:!!Female:!!Under 5 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_under_5",
            },
            "P12_028N": {
                "long_name": "!!Total:!!Female:!!5 to 9 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_5_to_9",
            },
            "P12_029N": {
                "long_name": "!!Total:!!Female:!!10 to 14 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_10_to_14",
            },
            "P12_030N": {
                "long_name": "!!Total:!!Female:!!15 to 17 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_15_to_17",
            },
            "P12_044N": {
                "long_name": "!!Total:!!Female:!!65 and 66 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_65_to_66",
            },
            "P12_045N": {
                "long_name": "!!Total:!!Female:!!67 to 69 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_67_to_69",
            },
            "P12_046N": {
                "long_name": "!!Total:!!Female:!!70 to 74 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_70_to_74",
            },
            "P12_047N": {
                "long_name": "!!Total:!!Female:!!75 to 79 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_75_to_79",
            },
            "P12_048N": {
                "long_name": "!!Total:!!Female:!!80 to 84 years: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_80_to_84",
            },
            "P12_049N": {
                "long_name": "!!Total:!!Female:!!85 years and over: SEX BY AGE FOR SELECTED AGE CATEGORIES",
                "short_name": "f_85_plus",
            },
            # race / ethnicity
            "P9_001N": {
                "long_name": "!!Total", # total from P9 table only: HISPANIC OR LATINO, AND NOT HISPANIC OR LATINO BY RACE
                "short_name": "total_p9",
            },
            "P9_002N": {
                "long_name": "!!Total:!!Hispanic or Latino",
                "short_name": "hispanic_latino",
            },
            "P9_005N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone",
                "short_name": "white",
            },
            "P9_006N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone",
                "short_name": "african_american",
            },
            "P9_007N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of one race:!!American Indian and Alaska Native alone",
                "short_name": "amer_indian_ak_native",
            },
            "P9_008N": {
                "long_name": "!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone",
                "short_name": "asian",
            },
            "P9_009N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone",
                "short_name": "hawaiian_pacislander",
            },
            "P9_010N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone",
                "short_name": "other",
            },
            "P9_011N": {
                "long_name": "!!Total:!!Not Hispanic or Latino:!!Population of two or more races:",
                "short_name": "multi",
            },
        },
    },
    "acs5": {
        "url": "https://api.census.gov/data/2020/acs/acs5/subject",# note that if any variable not found in a "subject" table is used, this base URL will need to be re-configured!
        "vars": {
            "S1810_C03_001E": {
                "long_name": "Percent with a disability!!Estimate!!Total civilian noninstitutionalized population",
                "short_name": "pct_w_disability",
            },
            "S1810_C03_001M": {
                "long_name": "Margin of Error!!Percent with a disability!!Total civilian noninstitutionalized population",
                "short_name": "moe_pct_w_disability",
            },
            "S2701_C03_001E": {
                "long_name": "Estimate!!Percent Insured!!Civilian noninstitutionalized population",
                "short_name": "pct_insured",
            },
            "S2701_C03_001M": {
                "long_name": "Margin of Error!!Percent Insured!!Civilian noninstitutionalized population	",
                "short_name": "moe_pct_insured",
            },
            "S2701_C05_001E": {
                "long_name": "Estimate!!Percent Uninsured!!Civilian noninstitutionalized population",
                "short_name": "pct_uninsured",
            },
            "S2701_C05_001M": {
                "long_name": "Margin of Error!!Percent Uninsured!!Civilian noninstitutionalized population	",
                "short_name": "moe_pct_uninsured",
            },
        },
    },
    "cdc": {
        "PLACES":{
            "url":{
                "county":"https://data.cdc.gov/resource/swc5-untb.json",
                "place":"https://data.cdc.gov/resource/eav7-hnsx.json",
                "zcta":"https://data.cdc.gov/resource/qnzd-25i4.json",
                "tract":"https://data.cdc.gov/resource/cwsq-ngmh.json",
            },
            "vars": {
                "CASTHMA": {
                    "long_name": "Current asthma among adults aged >=18 years",
                    "short_name": "pct_asthma",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
                "COPD": {
                    "long_name": "Chronic obstructive pulmonary disease among adults aged >=18 years",
                    "short_name": "pct_copd",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
                "CHD": {
                    "long_name": "Coronary heart disease among adults aged >=18 years",
                    "short_name": "pct_hd",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
                "STROKE": {
                    "long_name": "Stroke among adults aged >=18 years",
                    "short_name": "pct_stroke",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
                "DIABETES": {
                    "long_name": "Diagnosed diabetes among adults aged >=18 years",
                    "short_name": "pct_diabetes",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
                "KIDNEY": {
                    "long_name": "Chronic kidney disease among adults aged >=18 years",
                    "short_name": "pct_kd",
                    "data_value_type_name": "crude prevalence",
                    "data_value_type_id": "CrdPrv",
                },
            },
        },
        "SDOH":{
            "url":{
                "county":"https://data.cdc.gov/resource/i6u4-y3g4.json",
                "place":"https://data.cdc.gov/resource/edkk-ze78.json",
                "zcta":"https://data.cdc.gov/resource/bumh-rgsq.json",
                "tract":"https://data.cdc.gov/resource/e539-uadk.json",
            },
            "vars": {
                "REMNRTY": {
                    "long_name": "Persons of racial or ethnic minority status",
                    "short_name": "pct_minority",
                },
                "NOHSDP": {
                    "long_name": "No high school diploma among adults aged 25 years or older",
                    "short_name": "pct_no_hsdiploma",
                },
                "POV150": {
                    "long_name": "Persons living below 150% of the poverty level",
                    "short_name": "pct_below_150pov",
                },
                "BROAD": {
                    "long_name": "No broadband internet subscription among households",
                    "short_name": "pct_no_bband",
                },
            },
        },
    },
}

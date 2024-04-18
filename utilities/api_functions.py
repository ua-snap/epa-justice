import requests
import json
import pandas as pd
import numpy as np
from utilities.luts import *


def get_cdc_areatype_locationid_list(geoid_lu_df, gvv_id):
    """Get strings for area type and locationid's for a given GVV ID. GEOIDFQs are converted to CDC locationids.
    There will only be one area type, but strings may include more than one locationid. For CDC data, these are returned as a list.
    
    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs
        gvv_id (str): GVV ID used to look up associated GEOIDFQ(s)
    Returns:
        Tuple including geography type for API query (e.g., "place") and list of locationid strings in that geography type to use in API query.
        """
    areatypes = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['AREATYPE']
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes.to_list()[0] == 'County':
            areatype_str = 'county'
        elif areatypes.to_list()[0] in ['Census designated place', 'Incorporated place']:
            areatype_str = 'place'
        elif areatypes.to_list()[0] == 'ZCTA':
            areatype_str = 'zcta'
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['GEOIDFQ']
    if len(geoidfqs) > 1:
        if areatype_str == 'county':
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = []
            for geoidfq in geoidfqs.to_list():
                locationid_list.append(geoidfq[-5:])
        elif areatype_str == 'place':
            # get last 7 digits as state FIPS + place code
            locationid_list = []
            for geoidfq in geoidfqs.to_list():
                locationid_list.append(geoidfq[-7:])
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            locationid_list = []
            for geoidfq in geoidfqs.to_list():
                locationid_list.append(geoidfq[-5:])
    elif len(geoidfqs) == 1:
        if areatype_str == 'county':
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = [geoidfqs.to_list()[0][-5:]]
        elif areatype_str == 'place':
            # get last 7 digits as state FIPS + place code
            locationid_list = [geoidfqs.to_list()[0][-7:]]
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            locationid_list = [geoidfqs.to_list()[0][-5:]]
    else:
        # TODO: raise an error
        print("no associated GEOIDFQs found!")

    return areatype_str, locationid_list



def get_census_areatype_geoid_strings(geoid_lu_df, gvv_id):
    """Get strings for area type and fully qualified GEOIDFQs for a given GVV ID. 
    There will only be one area type, but strings may include more than one GEOIDFQ.
    
    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs
        gvv_id (str): GVV ID used to look up associated GEOIDFQ(s)
    Returns:
        Tuple including geography type for API query (e.g., "place") and string of GEOIDFQ id(s) in that geography type to use in API query
        """
    areatypes = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['AREATYPE']
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes.to_list()[0] == 'County':
            areatype_str = 'county'
        elif areatypes.to_list()[0] in ['Census designated place', 'Incorporated place']:
            areatype_str = 'place'
        elif areatypes.to_list()[0] == 'ZCTA':
            areatype_str = 'zip%20code%20tabulation%20area'
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['GEOIDFQ']
    if len(geoidfqs) > 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            county_geoids = []
            for geoidfq in geoidfqs.to_list():
                county_geoids.append(geoidfq[-3:])
            geoidfq_str = (",").join(county_geoids)
        elif areatype_str == 'place':
            # get last 5 digits for place code
            place_geoids = []
            for geoidfq in geoidfqs.to_list():
                place_geoids.append(geoidfq[-5:])
            geoidfq_str = (",").join(place_geoids)
        elif areatype_str == 'zip%20code%20tabulation%20area':
            # get last 5 digits for zip code
            zctas = []
            for geoidfq in geoidfqs.to_list():
                zctas.append(geoidfq[-5:])
            geoidfq_str = (",").join(zctas)
    elif len(geoidfqs) == 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            geoidfq_str = geoidfqs.to_list()[0][-3:]
        elif areatype_str == 'place':
            # get last 5 digits for place code
            geoidfq_str = geoidfqs.to_list()[0][-5:]
        elif areatype_str == 'zip%20code%20tabulation%20area':
            # get last 5 digits for zip code
            geoidfq_str = geoidfqs.to_list()[0][-5:]
    else:
        # TODO: raise an error
        print("no associated GEOIDFQs found!")

    return areatype_str, geoidfq_str


def compute_dhc(dhc_data):
    """Compute the population percentages for different age group combinations, and return the computed columns.
    This function makes a lot of assumptions about columns names; any revisions to the short names in luts.py will require changes here too."""
    #dhc_data['total_population'] = dhc_data['total_male'] + dhc_data['total_female']
    dhc_data['m_under_18'] = dhc_data['m_under_5'] + dhc_data['m_5_to_9'] + dhc_data['m_10_to_14'] + dhc_data['m_15_to_17']
    dhc_data['f_under_18'] = dhc_data['f_under_5'] + dhc_data['f_5_to_9'] + dhc_data['f_10_to_14'] + dhc_data['f_15_to_17']
    dhc_data['total_under_18'] = dhc_data['m_under_18'] + dhc_data['f_under_18']
    dhc_data['m_65_plus'] = dhc_data['m_65_to_66'] + dhc_data['m_67_to_69'] + dhc_data['m_70_to_74'] + dhc_data['m_75_to_79'] + dhc_data['m_80_to_84'] + dhc_data['m_85_plus']
    dhc_data['f_65_plus'] = dhc_data['f_65_to_66'] + dhc_data['f_67_to_69'] + dhc_data['f_70_to_74'] + dhc_data['f_75_to_79'] + dhc_data['f_80_to_84'] + dhc_data['f_85_plus']
    dhc_data['total_65_plus'] = dhc_data['m_65_plus'] + dhc_data['f_65_plus']
    dhc_data['pct_65_plus'] = dhc_data['total_65_plus']/dhc_data['total_population']*100
    dhc_data['pct_under_18'] = dhc_data['total_under_18']/dhc_data['total_population']*100

    return dhc_data[['GEOID', 'total_population', 'pct_65_plus', 'pct_under_18']]


def compute_acs5(acs5_data):
    """Compute ACS data columns."""
    # no computation necessary at the moment, but keeping this as a placeholder.
    return acs5_data


def fetch_census_data_and_compute(survey_id, gvv_id, geoid_lu_df):
    """Fetch census data from their API. Using the census survey id, joins a base URL to a list of variable codes, area type, and GEOIDFQ(s),
    and requests the URL. Returns the JSON response. Print an error message if no response.
    
    Args:
        survey_id (str): census survey id, one of "dhc" or "acs5"
        gvvid (str): GVV ID used to look up associated GEOIDFQ(s) and fetch data
        geoid_lu_df (pandas.DataFrame): lookup table with GVV IDs and GEOIDFQs
    Returns:
        pandas.DataFrame
    """
    # get strings to build URL
    base_url = var_dict[survey_id]["url"]
    var_str = (',').join(list(var_dict[survey_id]["vars"].keys()))
    areatype_str, geoidfq_str = get_census_areatype_geoid_strings(geoid_lu_df, gvv_id)

    # exclude state code from query if ZCTA
    if areatype_str not in ["place", "county"]:
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&key={api_key}"
    # otherwise include state FIPS code "02" for Alaska
    else:
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&in=state:02&key={api_key}"
    
    print(f"fetching data from: {url}")

    # request the data, raise error if not returned
    with requests.get(url) as r:
        if r.status_code != 200:
            #TODO: raise error
            print("No response, check your URL")
        else:
            r_json = r.json()
    # convert to dataframe and reformat
    df = pd.DataFrame(r_json[1:], columns=r_json[0]).astype(float)
    # rename geo column
    geolist = ["state", "place", "county", "zip code tabulation area"]
    for c in df.columns:
        if c in geolist:
            df.rename(columns={c:"GEOID"}, inplace=True)
    # use short names for variables columns if they exist in the dict
    new_cols_dict = {}
    for col in df.columns:
        try:
            new_col = var_dict[survey_id]["vars"][col]["short_name"]
            new_cols_dict[col]=new_col
        except:
            new_cols_dict[col]=col 
    df.rename(columns=new_cols_dict, inplace=True)
    # change any negative data values to NA... -6666666 is a commonly used nodata value, but there may be others that are not negative
    df.where(df >= 0, np.nan, inplace=True)


    #TODO: compute tables based on survey id
    if survey_id == "dhc":
        return compute_dhc(df)
    elif survey_id == "acs5":
        return compute_acs5(df)


def fetch_cdc_data_and_compute(gvv_id, geoid_lu_df):
    """Fetch CDC data from their API. Depending on the geography, joins a base URL to a individual variable codes and locationid(s),
    and requests the URL. Returns the JSON response. Print an error message if no response.
    
    Args:
        gvvid (str): GVV ID used to look up associated GEOIDFQ(s) and fetch data
        geoid_lu_df (pandas.DataFrame): lookup table with GVV IDs and GEOIDFQs
    Returns:
        pandas.DataFrame
    """
    # get strings to build URL
    areatype_str, locationid_list = get_cdc_areatype_locationid_list(geoid_lu_df, gvv_id)

    results = {}
    # do a separate query for each var and location and collect results
    for locationid in locationid_list:
        loc_results = {}
        for var_str in var_dict["cdc"]["PLACES"]["vars"].keys():
            base_url = var_dict["cdc"]["PLACES"]["url"][areatype_str]
            if areatype_str in ["county", "place"]:
                data_val_type_str = var_dict["cdc"]["PLACES"]["vars"][var_str]["data_value_type_id"]
                url = f"{base_url}?measureid={var_str}&datavaluetypeid={data_val_type_str}&locationid={locationid}"
            else:
                # do not specify data value type id... only crude prevalence is available for ZCTAs!
                url = f"{base_url}?measureid={var_str}&locationid={locationid}"
            
            print(f"fetching data from: {url}")

            with requests.get(url) as r:
                if r.status_code != 200:
                    #TODO: raise error
                    print("No response, check your URL")
                else:
                    r_json = r.json()
            try:
                val = r_json[0]['data_value']
            except:
                val = "NA"

            short_name = var_dict["cdc"]["PLACES"]["vars"][var_str]["short_name"]
            loc_results[short_name] = val

        for var_str in var_dict["cdc"]["SDOH"]["vars"].keys():
            base_url = var_dict["cdc"]["SDOH"]["url"][areatype_str]
            if areatype_str in ["county", "place"]:
                url = f"{base_url}?measureid={var_str}&locationid={locationid}"
            else:
                url = f"{base_url}?measureid={var_str}&locationid={locationid}"

            print(f"fetching data from: {url}")

            with requests.get(url) as r:
                if r.status_code != 200:
                    #TODO: raise error
                    print("No response, check your URL")
                else:
                    r_json = r.json()
            try:
                val = r_json[0]['data_value']
            except:
                val = "NA"

            short_name = var_dict["cdc"]["SDOH"]["vars"][var_str]["short_name"]
            loc_results[short_name] = val
        results[locationid] = loc_results

        results_df = pd.DataFrame.from_dict(results, orient='index')

    return results_df

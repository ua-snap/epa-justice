import requests
import json
import pandas as pd
import numpy as np
from multiprocessing.pool import Pool
from utilities.luts import *



def create_comment_dict(geoid_lu_df):
    """Given the lookup table, create the comments based on actual table relationships.

    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs, census places, and comments
    Returns:
        dictionary with GVV ID's as keys and comments as values
    """
    df = geoid_lu_df
    comment_dict = {}

    for index_, row in df.iterrows():
        # exclude NaNs from commenting
        if not isinstance(row['COMMENT'], float):

            # deal with one-to-many tract situation first
            if row['AREATYPE'] == 'Census tract':
                # if >1 GVV place associated with this row, list place names (census tracts) in the comment
                sub_df = df[df['name'] == row['name']]
                if len(sub_df) > 1:
                    tract_list = sub_df['PLACENAME'].tolist()
                    if len(tract_list) == 2:
                        tracts = (" and ").join(tract_list)
                        comment = f"Data for this place represent multiple merged census tracts: {tracts}"
                    elif len(tract_list) > 2:
                        tract_list[-1] = str("and " + tract_list[-1])
                        tracts = (", ").join(tract_list)
                        comment = f"Data for this place represent multiple merged census tracts: {tracts}"
            else:
                sub_df = df[df['PLACENAME'] == row['PLACENAME']]
                name_list = sub_df['name'].tolist()

                if len(name_list) == 1:
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {name_list[0]}."
                elif len(name_list) == 2:
                    names = (" and ").join(name_list)
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {names}."
                elif len(name_list) > 2:
                    name_list[-1] = str("and " + name_list[-1])
                    names = (", ").join(name_list)
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {names}."
            
            comment_dict[row['id']] = comment
        
        else:

            comment_dict[row['id']] = ""
    
    return comment_dict
                        

def fetch_and_merge(geoid_lu_df, gvv_id, comment_dict):
    """Given the lookup table and GVV ID, fetches all data and merges the results into a dataframe.

    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs
        gvv_id (str): GVV ID used to look up associated GEOIDFQ(s)
        comment_dict (dictionary): dictionary with GVV ID's as keys and comments as values
    Returns:
        pandas.DataFrame
    """
    geoids = get_standard_geoid_df(geoid_lu_df, gvv_id)
    dhc = fetch_census_data_and_compute("dhc", gvv_id, geoid_lu_df)
    acs5 = fetch_census_data_and_compute("acs5", gvv_id, geoid_lu_df)
    cdc = fetch_cdc_data_and_compute(gvv_id, geoid_lu_df)

    df = geoids.merge(dhc, how="left", left_on="GEOID", right_on="GEOID").merge(
        acs5, how="left", left_on="GEOID", right_on="GEOID").merge(
            cdc, how="left", left_on="GEOID", right_on="locationid")
    
    df['comment'] = ""

    for index, row in df.iterrows():
        df.loc[index, 'comment'] = comment_dict[row['id']]
    
    return df


def run_fetch_and_merge(geoid_lu_df):
    """Use multiprocessing to run the fetch and merge functions.
    Collected results will be concatenated.

    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs, census places, and comments
    Returns:
        pandas.DataFrame
    """
    # create dict
    comment_dict = create_comment_dict(geoid_lu_df)
    # create a list of tuples to use as arguments in the fetch_and_merge() function
    arg_tuples = []
    for gvv_id in list(geoid_lu_df.id.unique()):
        arg_tuple = geoid_lu_df, gvv_id, comment_dict
        arg_tuples.append(arg_tuple)
    # collect results from all tuple args    
    results=[]
    with Pool() as pool:
        for result in pool.starmap(fetch_and_merge, arg_tuples):
            results.append(result)
    # concatenate results and return the dataframe
    return pd.concat(results)


def get_standard_geoid_df(geoid_lu_df, gvv_id):
    """Create a simple dataframe of requested GEOIDS, with no state FIPS code.
    All results tables will be joined to this table.

    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs
        gvv_id (str): GVV ID used to look up associated GEOIDFQ(s)
    Returns:
        pandas.DataFrame
    """
    # list GVV IDs & names (they should be dups if >1)
    gvv_ids = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['id'].to_list()
    gvv_names = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['name'].to_list()
    # list census area placename, areatype, and geoidfqs (should be unique if >1)
    placenames = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['PLACENAME'].to_list()
    areatypes = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['AREATYPE'].to_list()
    geoidfqs = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['GEOIDFQ'].to_list()

    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == 'County':
            areatype_str = 'county'
        elif areatypes[0] in ['Census designated place', 'Incorporated place']:
            areatype_str = 'place'
        elif areatypes[0] == 'ZCTA':
            areatype_str = 'zcta'
        elif areatypes[0] == 'Census tract':
            areatype_str = 'tract'
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    if len(geoidfqs) > 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-3:])
        elif areatype_str == 'place':
            # get last 5 digits as place code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-5:])
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-5:])
        elif areatype_str == 'tract':
            # get last 9 digits for county FIPS code + tract code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-9:])

    elif len(geoidfqs) == 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            geoid_list = [geoidfqs[0][-3:]]
        elif areatype_str == 'place':
            # get last 5 digits as place code
            geoid_list = [geoidfqs[0][-5:]]
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            geoid_list = [geoidfqs[0][-5:]]
        elif areatype_str == 'tract':
            # get last 7 digits for county FIPS code + tract code
            geoid_list = [geoidfqs[0][-9:]]

    else:
        # TODO: raise an error
        print("no associated GEOIDFQs found!")


    df = pd.DataFrame(zip(gvv_ids, gvv_names, areatypes, placenames, geoid_list), 
                      columns=["id", "name", "areatype", "placename", "GEOID"])
    
    return df


def get_cdc_areatype_locationid_list(geoid_lu_df, gvv_id):
    """Get strings for area type and locationid's for a given GVV ID. GEOIDFQs are converted to CDC locationids.
    There will only be one area type, but strings may include more than one locationid. For CDC data, these are returned as a list.
    
    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs
        gvv_id (str): GVV ID used to look up associated GEOIDFQ(s)
    Returns:
        Tuple including geography type for API query (e.g., "place") and list of locationid strings in that geography type to use in API query.
        """
    areatypes = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['AREATYPE'].to_list()
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == 'County':
            areatype_str = 'county'
        elif areatypes[0] in ['Census designated place', 'Incorporated place']:
            areatype_str = 'place'
        elif areatypes[0] == 'ZCTA':
            areatype_str = 'zcta'
        elif areatypes[0] == 'Census tract':
            areatype_str = 'tract'
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['GEOIDFQ'].to_list()
    if len(geoidfqs) > 1:
        if areatype_str == 'county':
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-5:])
        elif areatype_str == 'place':
            # get last 7 digits as state FIPS + place code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-7:])
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-5:])
        elif areatype_str == 'tract':
            # get last 11 digits as state FIPS + county FIPS code + tract code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-11:])

    elif len(geoidfqs) == 1:
        if areatype_str == 'county':
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = [geoidfqs[0][-5:]]
        elif areatype_str == 'place':
            # get last 7 digits as state FIPS + place code
            locationid_list = [geoidfqs[0][-7:]]
        elif areatype_str == 'zcta':
            # get last 5 digits for zip code
            locationid_list = [geoidfqs[0][-5:]]
        elif areatype_str == 'tract':
            # get last 11 digits as state FIPS + county FIPS code + tract code
            locationid_list = [geoidfqs[0][-11:]]

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
    areatypes = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['AREATYPE'].to_list()
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == 'County':
            areatype_str = 'county'
        elif areatypes[0] in ['Census designated place', 'Incorporated place']:
            areatype_str = 'place'
        elif areatypes[0] == 'ZCTA':
            areatype_str = 'zip%20code%20tabulation%20area'
        elif areatypes[0] == 'Census tract':
            areatype_str = 'tract'
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df['id'] == gvv_id]['GEOIDFQ'].to_list()
    if len(geoidfqs) > 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            county_geoids = []
            for geoidfq in geoidfqs:
                county_geoids.append(geoidfq[-3:])
            geoidfq_str = (",").join(county_geoids)
        elif areatype_str == 'place':
            # get last 5 digits for place code
            place_geoids = []
            for geoidfq in geoidfqs:
                place_geoids.append(geoidfq[-5:])
            geoidfq_str = (",").join(place_geoids)
        elif areatype_str == 'zip%20code%20tabulation%20area':
            # get last 5 digits for zip code
            zctas = []
            for geoidfq in geoidfqs:
                zctas.append(geoidfq[-5:])
            geoidfq_str = (",").join(zctas)
        elif areatype_str == 'tract':
            # get digits -9 thru -6 as county FIPS code (for first tract only - assumes all tracts are in same county) 
            # get last 6 digits as tract code
            county_geoid = geoidfqs[0][-9:-6]
            tracts = []
            for geoidfq in geoidfqs:
                tracts.append(geoidfq[-6:])
            # return as list: tract is a special case that will be checked for in fetch_census_data_and_compute()
            geoidfq_str = [county_geoid, (",").join(tracts)]


    elif len(geoidfqs) == 1:
        if areatype_str == 'county':
            # get last 3 digits as county FIPS code
            geoidfq_str = geoidfqs[0][-3:]
        elif areatype_str == 'place':
            # get last 5 digits for place code
            geoidfq_str = geoidfqs[0][-5:]
        elif areatype_str == 'zip%20code%20tabulation%20area':
            # get last 5 digits for zip code
            geoidfq_str = geoidfqs[0][-5:]
        elif areatype_str == 'tract':
            # get digits -9 thru -6 as county FIPS code (for first tract only - assumes all tracts are in same county) 
            # get last 6 digits as tract code
            county_geoid = geoidfqs[0][-9:-6]
            tract_geoid = geoidfqs[0][-6:]
            # return as list: tract is a special case that will be checked for in fetch_census_data_and_compute()
            geoidfq_str = [county_geoid, tract_geoid]

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
    dhc_data['pct_65_plus'] = round(dhc_data['total_65_plus']/dhc_data['total_population']*100,2)
    dhc_data['pct_under_18'] = round(dhc_data['total_under_18']/dhc_data['total_population']*100,2)

    return dhc_data[['GEOID', 'total_population', 'pct_65_plus', 'pct_under_18']]


def compute_acs5(acs5_data):
    """Compute ACS data columns."""
    # no computation necessary at the moment, but keeping this as a placeholder.
    return acs5_data


def compute_cdc(cdc_data):
    """Compute CDC data columns."""
    # no computation necessary at the moment, but keeping this as a placeholder.
    return cdc_data


def fetch_census_data_and_compute(survey_id, gvv_id, geoid_lu_df, print_url=False):
    """Fetch census data from their API. Using the census survey id, joins a base URL to a list of variable codes, area type, and GEOIDFQ(s),
    and requests the URL. Returns the JSON response. Print an error message if no response.
    
    Args:
        survey_id (str): census survey id, one of "dhc" or "acs5"
        gvvid (str): GVV ID used to look up associated GEOIDFQ(s) and fetch data
        geoid_lu_df (pandas.DataFrame): lookup table with GVV IDs and GEOIDFQs
        print (bool): whether or not to print URLs for QC
    Returns:
        pandas.DataFrame
    """
    # get strings to build URL
    base_url = var_dict[survey_id]["url"]
    var_str = (',').join(list(var_dict[survey_id]["vars"].keys()))
    areatype_str, geoidfq_str = get_census_areatype_geoid_strings(geoid_lu_df, gvv_id)

    # exclude state code from query if ZCTA
    if areatype_str == "zcta":
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&key={api_key}"
    # separate list to get county and tract strings, include state FIPS code "02" for Alaska
    elif areatype_str == "tract":
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str[1]}&in=state:02&in=county:{geoidfq_str[0]}&key={api_key}"
    # otherwise include state FIPS code "02" for Alaska
    else:
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&in=state:02&key={api_key}"
    
    if print_url: print(f"Requesting US Census data from: {url}")

    # request the data, raise error if not returned
    with requests.get(url) as r:
        if r.status_code != 200:
            #TODO: raise error
            print("No response, check your URL")
        else:
            r_json = r.json()

    # convert to dataframe and reformat
    df = pd.DataFrame(r_json[1:], columns=r_json[0])

    # rename geo column depending on areatype
    if areatype_str == 'tract':
        df['GEOID'] = df['county'] + df ['tract'] # concatenate columns to get standard 9 digit tract code
        df.drop(columns=["state", "county", "tract"], inplace=True)
    else:
        geolist = ["place", "county", "zip code tabulation area"]
        for c in df.columns:
            if c in geolist:
                df.rename(columns={c:"GEOID"}, inplace=True)
            if c == "state":
                df.drop(columns="state", inplace=True)

    # convert non-GEOID columns to floats, and change any negative data values to NA...
    # -6666666 is a commonly used nodata value, but there may be others. Assume all positive values are valid.
    for c in df.columns:
        if c != "GEOID":
            df[c] = df[c].astype(float)
            df[c].where(df[c] > 0, np.nan, inplace=True)

    # use short names for variables columns if they exist in the dict
    new_cols_dict = {}
    for col in df.columns:
        try:
            new_col = var_dict[survey_id]["vars"][col]["short_name"]
            new_cols_dict[col]=new_col
        except:
            new_cols_dict[col]=col 
    df.rename(columns=new_cols_dict, inplace=True)

    # compute tables based on survey id
    if survey_id == "dhc":
        return compute_dhc(df)
    elif survey_id == "acs5":
        return compute_acs5(df)


def fetch_cdc_data_and_compute(gvv_id, geoid_lu_df, print_url=False):
    """Fetch CDC data from their API. Depending on the geography, joins a base URL to a individual variable codes and locationid(s),
    and requests the URL. Returns the JSON response. Print an error message if no response.
    
    Args:
        gvvid (str): GVV ID used to look up associated GEOIDFQ(s) and fetch data
        geoid_lu_df (pandas.DataFrame): lookup table with GVV IDs and GEOIDFQs
        print (bool): whether or not to print URLs for QC
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
                url = f"{base_url}?$$app_token={app_token}&measureid={var_str}&datavaluetypeid={data_val_type_str}&locationid={locationid}"
            else:
                # do not specify data value type id... only crude prevalence is available for ZCTAs!
                url = f"{base_url}?$$app_token={app_token}&measureid={var_str}&locationid={locationid}"

            if print_url: print(f"Requesting CDC data from: {url}")

            with requests.get(url) as r:
                if r.status_code != 200:
                    #TODO: raise error
                    print("No response, check your URL")
                else:
                    r_json = r.json()
            try:
                val = float(r_json[0]['data_value'])
            except:
                val = np.nan

            short_name = var_dict["cdc"]["PLACES"]["vars"][var_str]["short_name"]
            loc_results[short_name] = val

        for var_str in var_dict["cdc"]["SDOH"]["vars"].keys():
            base_url = var_dict["cdc"]["SDOH"]["url"][areatype_str]
            if areatype_str in ["county", "place"]:
                url = f"{base_url}?$$app_token={app_token}&measureid={var_str}&locationid={locationid}"
            else:
                url = f"{base_url}?$$app_token={app_token}&measureid={var_str}&locationid={locationid}"
            
            if print_url: print(f"Requesting CDC data from: {url}")

            with requests.get(url) as r:
                if r.status_code != 200:
                    #TODO: raise error
                    print("No response, check your URL")
                else:
                    r_json = r.json()
            try:
                val = float(r_json[0]['data_value'])
            except:
                val = np.nan

            short_name = var_dict["cdc"]["SDOH"]["vars"][var_str]["short_name"]
            loc_results[short_name] = val

        # standardize locationid to match geoids for joining later on
        # removes state FIPS for county, place, and tract; should not affect zip codes
        if locationid.startswith("02"):
            locationid = locationid[2:]
        results[locationid] = loc_results

        #convert to dataframe and reformat
        df = pd.DataFrame.from_dict(results, orient='index', )
        # rename/reindex loc col
        df.reset_index(names='locationid', inplace=True)       
        # change any negative data values to NA
        # nodata values are also introduced above for any empty returns
        for c in df.columns:
            if c != "locationid":
                df[c] = df[c].astype(float)
                df[c].where(df[c] > 0, np.nan, inplace=True)

    return compute_cdc(df)
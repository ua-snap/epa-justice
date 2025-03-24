import requests
import pandas as pd
import numpy as np
import math
from multiprocessing.pool import Pool
from utilities.luts import *
from functools import reduce


def calculate_pop_variance(df):
    """Calculates the adult population variance for each measure into new columns in the final results table.
    This is done by back-calculating the standard deviation for each measure and multiplying
    it's square by the adult population - 1. Also adds the total adult population count as a new column in the final results table.

    Args:
        df (pandas.DataFrame): concatenated dataframe result from the run_fetch_and_merge() function.
    Returns:
        pandas.DataFrame with adult population variance for each measure and total adult population columns added.
    """

    # add a new column with each row's adult population count
    df["adult_population"] = round(
        df["total_population"] - (df["total_population"] * (df["pct_under_18"] / 100))
    )

    # add new empty columns to hold adult population variance for each measure
    for var in var_dict["cdc"]["PLACES"]["vars"]:
        short_name = var_dict["cdc"]["PLACES"]["vars"][var]["short_name"]
        df[short_name + "_adult_population_variance"] = None

    # iterate through the rows and measures to back calculate the standard deviation for the adult population
    for i, row in df.iterrows():
        for var in var_dict["cdc"]["PLACES"]["vars"]:
            # identify the columns for the measure and the high CI
            measure_col = var_dict["cdc"]["PLACES"]["vars"][var]["short_name"]
            ci_high_col = str(measure_col + "_high")
            # find the difference between high CI and the measure value (ie, the margin of error)
            moe = row[ci_high_col] - row[measure_col]
            # multiply moe by square root of adult population and divide by 1.96 to get the standard deviation for 95% CI
            sd = (moe * (math.sqrt(row["adult_population"]))) / 1.96
            # calculate variance and adult population variance, then populate the empty columns we created
            # formula = (adult_population - 1) * variance
            variance = sd**2
            adult_population_variance = (row["adult_population"] - 1) * variance
            df[measure_col + "_adult_population_variance"].iloc[
                i
            ] = adult_population_variance

    return df


def aggregate_results(results_df):
    """Aggregates any one-to-many relationships in the final results table.
    Includes calculating the pooled standard deviation and the 90% CI for each measure that reports those statistics.

    Args:
        df (pandas.DataFrame): concatenated dataframe result from the run_fetch_and_merge() function
    Returns:
        pandas.DataFrame with any one-to-many entries aggregated into one-to-one entries
    """
    # reset index just in case there are duplicate indices
    df = results_df.reset_index(drop=True)

    # calculate adult population variances (adds a new column to the dataframe for each measure)
    # required for calculation of pooled 90% CI
    df = calculate_pop_variance(df)

    # make sure GEOIDs are strings in order to list them with sum (instead of summing them as integers!)
    df["GEOID"] = df["GEOID"].astype(str)

    # create wrapper to concatenate strings for placename and GEOID columns
    # this info will already be preserved in the comments, but also need to list in these columns to be more explicit
    def concat_strings(x):
        return (", ").join(x)

    # create wrapper to use np.sum, or return NA if any NA values exist
    def sum_no_nan(x):
        if x.isna().any():
            return np.nan
        else:
            return np.sum(x)

    # create wrapper to use np.nanmean
    def nanmean(x):
        return np.nanmean(x)

    # create function to replace negative values with 0
    def replace_negatives(x):
        return max(0, x)

    # create moe aggregation function
    # as defined here: https://www.census.gov/content/dam/Census/library/publications/2018/acs/acs_general_handbook_2018_ch08.pdf
    def aggregate_moe(x):
        if x.isna().any():
            return np.nan
        else:
            return math.sqrt(np.sum(x**2))

    # build an aggregation dict for all data columns
    # use "first" for non-data columns, or the custom function when aggregating placename and GEOID strings
    # list columns that are only for adult population; we need to use the "adult_population" field when aggregating these
    # list columns that have MOE values; we need to aggregate these according the formula defined above
    # lis columns that do not deal with population at all (pct of housing units, etc...)... these will simply be averaged
    # sum all other data columns; they will be converted from pct to real population counts before summing
    non_data_cols = ["id", "name", "areatype", "placename", "GEOID", "comment"]

    adult_only_cols = [
        "pct_asthma",
        "pct_copd",
        "pct_chd",
        "pct_stroke",
        "pct_diabetes",
        "pct_mh",
        "pct_foodstamps",
        "pct_emospt",
    ]

    moe_cols = [
        "moe_pct_w_disability",
        "moe_pct_insured",
        "moe_pct_uninsured",
        "pct_no_bband_moe",
        "pct_crowding_moe",
        "pct_hcost_moe",
        "pct_no_hsdiploma_moe",
        "pct_below_150pov_moe",
        "pct_minority_moe",
        "pct_single_parent_moe",
        "pct_unemployed_moe",
    ]

    non_pop_cols = [
        "pct_no_bband",
        "pct_crowding",
        "pct_hcost",
        "pct_single_parent",
    ]

    agg_dict = {}

    for col in df.columns:
        if col in non_data_cols:
            if col in ["placename", "GEOID"]:
                agg_dict[col] = concat_strings
            else:
                agg_dict[col] = "first"
        elif col in moe_cols:
            agg_dict[col] = aggregate_moe
        elif col in non_pop_cols:
            agg_dict[col] = nanmean
        else:
            agg_dict[col] = sum_no_nan

    # create list to collect results
    agg_df_list = []

    # list duplicated ids and names; these rows need to be aggregated
    dups = list(
        zip(
            df[df.duplicated(subset="id")]["id"].unique().tolist(),
            df[df.duplicated(subset="id")]["name"].unique().tolist(),
        )
    )

    # iterate thru the list of duplicated ids and names
    for dup in dups:
        print(f"Aggregating values for {dup[0]}: {dup[1]}")

        # get duplicated rows into a subset dataframe
        sub_df = df[df["id"] == dup[0]]

        # calculate the pooled SD for each measure and add as a new column to each row in the subset dataframe
        # note that these values will just be identical in every row of the subset dataframe, so that we can use "first" aggregation method
        for col in sub_df.columns:
            if col.endswith("_adult_population_variance"):
                pooled_sd_col_name = "pct_" + col.split("_")[1] + "_pooled_sd"
                # calculate the pooled SD for each measure
                # formula = sqrt(sum of variances / sum of adult populations - count of rows)
                sum_of_variances = np.sum(sub_df[col])
                sum_of_adult_populations = np.sum(sub_df["adult_population"])
                count_of_rows = len(sub_df)
                pooled_sd = np.sqrt(
                    sum_of_variances / (sum_of_adult_populations - count_of_rows)
                )

                sub_df[pooled_sd_col_name] = pooled_sd
                agg_dict[pooled_sd_col_name] = "first"

        # compute population counts by row, only for columns dealing with population percentages
        for col in sub_df.columns:
            if (
                col != "total_population"
                and col != "adult_population"
                and col.endswith("pooled_sd") is False
                and col not in adult_only_cols
                and col not in moe_cols
                and col not in non_pop_cols
                and col not in non_data_cols
            ):
                sub_df[col] = sub_df["total_population"] * sub_df[col] / 100
            elif col in adult_only_cols:
                sub_df[col] = sub_df["adult_population"] * sub_df[col] / 100

        # in this version of sub_df, some of the columns are population counts and NOT percentages
        # so now we can use groupby and aggregate the data columns according to the agg_dict functions
        agg_df = sub_df.groupby("id").agg(agg_dict)

        # convert the columns dealing with population percentages from counts back to percentages
        for col in agg_df.columns:
            if (
                col != "total_population"
                and col != "adult_population"
                and col.endswith("pooled_sd") is False
                and col not in adult_only_cols
                and col not in moe_cols
                and col not in non_pop_cols
                and col not in non_data_cols
            ):
                agg_df[col] = round((agg_df[col] / agg_df["total_population"] * 100), 2)
            elif col in adult_only_cols:
                agg_df[col] = round((agg_df[col] / agg_df["adult_population"] * 100), 2)

        # calculate the pooled CI for each measure and replace the values in each row in the subset dataframe
        for col in agg_df.columns:
            if col.endswith("_high"):
                # drop the column (it already exists with an incorrect value)
                agg_df.drop(columns=col, inplace=True)
                # get the measure name from the column name and set up the new column name
                measure_name = col.split("_")[1]
                measure_col_name = "pct_" + measure_name
                pooled_sd_col_name = measure_col_name + "_pooled_sd"
                # use first value as pooled SD for each measure
                pooled_sd = agg_df[pooled_sd_col_name].values[0]
                # high 90% CI formula: value + (1.64 * (pooled SD / sqrt(adult population)))
                agg_df[col] = agg_df[measure_col_name] + (
                    1.64 * (pooled_sd / math.sqrt(agg_df["adult_population"].sum()))
                )
            elif col.endswith("_low"):
                # drop the column (it already exists with an incorrect value)
                agg_df.drop(columns=col, inplace=True)
                # get the measure name from the column name and set up the new column name
                measure_name = col.split("_")[1]
                measure_col_name = "pct_" + measure_name
                pooled_sd_col_name = measure_col_name + "_pooled_sd"
                # use first value as pooled SD for each measure
                pooled_sd = agg_df[pooled_sd_col_name].values[0]
                # low 90% CI formula: value - (1.64 * (pooled SD / sqrt(adult population)))
                agg_df[col] = agg_df[measure_col_name] - (
                    1.64 * (pooled_sd / math.sqrt(agg_df["adult_population"].sum()))
                )

        # drop the original duplicated rows
        df.drop(df[df["id"] == dup[0]].index, inplace=True)

        # add the newly aggregated rows to agg_df_list and concatenate them with the original dataframe
        agg_df_list.append(agg_df)

        df = pd.concat([df, *agg_df_list])
        df.reset_index(drop=True, inplace=True)

    for col in moe_cols:
        # subtract "_moe" from the col name if that substring is in the col name string
        if "_moe" in col:
            measure_name = col.split("_moe")[0]
        elif "moe_" in col:
            measure_name = col.split("moe_")[1]
        # set up the new column names
        high_col_name = measure_name + "_high"
        low_col_name = measure_name + "_low"
        # calculate high and low CI values
        df[high_col_name] = df[measure_name] + df[col]
        df[low_col_name] = df[measure_name] - df[col]
        df[low_col_name] = df[low_col_name].apply(
            replace_negatives
        )  # the low CI value cannot go below zero!

    # list columns we want to drop from the final results dataframe
    drop_cols = [
        col
        for col in df.columns
        if "pooled_sd" in col or "variance" in col or "moe" in col
    ]
    drop_cols += ["adult_population"]

    df = df.drop(columns=drop_cols)

    # round all data columns to 2 decimal places (ie columns not in non_data_cols)
    for col in df.columns:
        if col not in non_data_cols:
            df[col] = round(df[col], 2)

    return df


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
        if not isinstance(row["COMMENT"], float):
            # deal with one-to-many tract situation first
            if row["AREATYPE"] == "Census tract":
                # if >1 tract associated with single GVV place, list tracts in the comment
                sub_df = df[df["name"] == row["name"]]
                if len(sub_df) > 1:
                    tract_list = sub_df["PLACENAME"].tolist()
                    if len(tract_list) == 2:
                        tracts = (" and ").join(tract_list)
                        comment = f"Data for this place represent multiple merged census tracts: {tracts}"
                    elif len(tract_list) > 2:
                        tract_list[-1] = str("and " + tract_list[-1])
                        tracts = (", ").join(tract_list)
                        comment = f"Data for this place represent multiple merged census tracts: {tracts}"
                # if only one tract associated with single GVV place, use standard comment
                else:
                    name = sub_df["name"].tolist()[0]
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {name}."

            else:
                sub_df = df[df["PLACENAME"] == row["PLACENAME"]]
                name_list = sub_df["name"].tolist()

                if len(name_list) == 1:
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {name_list[0]}."
                elif len(name_list) == 2:
                    names = (" and ").join(name_list)
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {names}."
                elif len(name_list) > 2:
                    name_list[-1] = str("and " + name_list[-1])
                    names = (", ").join(name_list)
                    comment = f"Data represent information from nearest {row['AREATYPE'].lower()} ({row['PLACENAME']}), which includes {names}."

            comment_dict[row["id"]] = comment

        else:
            comment_dict[row["id"]] = ""

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

    df = (
        geoids.merge(dhc, how="left", left_on="GEOID", right_on="GEOID")
        .merge(acs5, how="left", left_on="GEOID", right_on="GEOID")
        .merge(cdc, how="left", left_on="GEOID", right_on="locationid")
    )

    # drop locationid column
    df.drop(columns="locationid", inplace=True)

    # add comments column and populate from comment dictionary
    df["comment"] = ""
    for index, row in df.iterrows():
        df.loc[index, "comment"] = comment_dict[row["id"]]

    return df


def run_fetch_and_merge(geoid_lu_df):
    """Use multiprocessing to run the fetch and merge functions.
    Collected results will be concatenated.

    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs, census places, and comments
    Returns:
        pandas.DataFrame
    """
    geoid_lu_df = add_ak_us(geoid_lu_df)

    # create dict
    comment_dict = create_comment_dict(geoid_lu_df)
    # create a list of tuples to use as arguments in the fetch_and_merge() function
    arg_tuples = []
    for gvv_id in list(geoid_lu_df.id.unique()):
        arg_tuple = geoid_lu_df, gvv_id, comment_dict
        arg_tuples.append(arg_tuple)
    # collect results from all tuple args
    results = []
    with Pool() as pool:
        for result in pool.starmap(fetch_and_merge, arg_tuples):
            results.append(result)
    # concatenate results and return the dataframe
    return pd.concat(results)


def add_ak_us(df):
    """Adds rows to the GVV lookup table for state of Alaska and entire US.
    Args:
        geoid_lu_df (pandas.DataFrame): table with GVV IDs and associated GEOIDFQs, census places, and comments
    Returns:
        pandas.DataFrame
    """
    # fields are: id,name,alt_name,region,country,latitude,longitude,type,GEOIDFQ,PLACENAME,AREATYPE,COMMENT
    ak_row = [
        "AK0",
        "Alaska",
        "",
        "",
        "",
        "",
        "",
        "",
        "0400000US02",
        "Alaska",
        "State",
        np.nan,
    ]
    us_row = [
        "US0",
        "United States",
        "",
        "",
        "",
        "",
        "",
        "",
        "0100000US",
        "United States",
        "Nation",
        np.nan,
    ]

    df.loc[len(df.index)] = ak_row
    df.loc[len(df.index)] = us_row

    return df


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
    gvv_ids = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["id"].to_list()
    gvv_names = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["name"].to_list()
    # list census area placename, areatype, and geoidfqs (should be unique if >1)
    placenames = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["PLACENAME"].to_list()
    areatypes = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["AREATYPE"].to_list()
    geoidfqs = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["GEOIDFQ"].to_list()

    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == "County":
            areatype_str = "county"
        elif areatypes[0] in ["Census designated place", "Incorporated place"]:
            areatype_str = "place"
        elif areatypes[0] == "ZCTA":
            areatype_str = "zcta"
        elif areatypes[0] == "Census tract":
            areatype_str = "tract"
        elif areatypes[0] == "State":
            areatype_str = "state"
        elif areatypes[0] == "Nation":
            areatype_str = "us"
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    if len(geoidfqs) > 1:
        if areatype_str == "county":
            # get last 3 digits as county FIPS code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-3:])
        elif areatype_str == "place":
            # get last 5 digits as place code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-5:])
        elif areatype_str == "zcta":
            # get last 5 digits for zip code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-5:])
        elif areatype_str == "tract":
            # get last 9 digits for county FIPS code + tract code
            geoid_list = []
            for geoidfq in geoidfqs:
                geoid_list.append(geoidfq[-9:])

    elif len(geoidfqs) == 1:
        if areatype_str == "county":
            # get last 3 digits as county FIPS code
            geoid_list = [geoidfqs[0][-3:]]
        elif areatype_str == "place":
            # get last 5 digits as place code
            geoid_list = [geoidfqs[0][-5:]]
        elif areatype_str == "zcta":
            # get last 5 digits for zip code
            geoid_list = [geoidfqs[0][-5:]]
        elif areatype_str == "tract":
            # get last 9 digits for county FIPS code + tract code
            geoid_list = [geoidfqs[0][-9:]]
        elif areatype_str == "state":
            # return 2 digit AK FIPs code
            geoid_list = ["02"]
        elif areatype_str == "us":
            # return 1 digit country code
            geoid_list = ["1"]

    else:
        # TODO: raise an error
        print("no associated GEOIDFQs found!")

    df = pd.DataFrame(
        zip(gvv_ids, gvv_names, areatypes, placenames, geoid_list),
        columns=["id", "name", "areatype", "placename", "GEOID"],
    )

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
    areatypes = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["AREATYPE"].to_list()
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == "County":
            areatype_str = "county"
        elif areatypes[0] in ["Census designated place", "Incorporated place"]:
            areatype_str = "place"
        elif areatypes[0] == "ZCTA":
            areatype_str = "zcta"
        elif areatypes[0] == "Census tract":
            areatype_str = "tract"
        elif areatypes[0] == "State":
            areatype_str = "state"
        elif areatypes[0] == "Nation":
            areatype_str = "us"
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["GEOIDFQ"].to_list()
    if len(geoidfqs) > 1:
        if areatype_str == "county":
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-5:])
        elif areatype_str == "place":
            # get last 7 digits as state FIPS + place code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-7:])
        elif areatype_str == "zcta":
            # get last 5 digits for zip code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-5:])
        elif areatype_str == "tract":
            # get last 11 digits as state FIPS + county FIPS code + tract code
            locationid_list = []
            for geoidfq in geoidfqs:
                locationid_list.append(geoidfq[-11:])

    elif len(geoidfqs) == 1:
        if areatype_str == "county":
            # get last 5 digits as state FIPS + county FIPS code
            locationid_list = [geoidfqs[0][-5:]]
        elif areatype_str == "place":
            # get last 7 digits as state FIPS + place code
            locationid_list = [geoidfqs[0][-7:]]
        elif areatype_str == "zcta":
            # get last 5 digits for zip code
            locationid_list = [geoidfqs[0][-5:]]
        elif areatype_str == "tract":
            # get last 11 digits as state FIPS + county FIPS code + tract code
            locationid_list = [geoidfqs[0][-11:]]
        elif areatype_str == "state":
            # return 2 digit AK FIPs code
            locationid_list = ["02"]
        elif areatype_str == "us":
            # return 1 digit country code
            locationid_list = ["1"]
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
    areatypes = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["AREATYPE"].to_list()
    if len(areatypes) == 0:
        # TODO: raise an error
        print("no associated AREATYPE found!")
    else:
        if areatypes[0] == "County":
            areatype_str = "county"
        elif areatypes[0] in ["Census designated place", "Incorporated place"]:
            areatype_str = "place"
        elif areatypes[0] == "ZCTA":
            areatype_str = "zip%20code%20tabulation%20area"
        elif areatypes[0] == "Census tract":
            areatype_str = "tract"
        elif areatypes[0] == "State":
            areatype_str = "state"
        elif areatypes[0] == "Nation":
            areatype_str = "us"
        else:
            # TODO: raise an error
            print("unrecognized AREATYPE!")

    geoidfqs = geoid_lu_df[geoid_lu_df["id"] == gvv_id]["GEOIDFQ"].to_list()
    if len(geoidfqs) > 1:
        if areatype_str == "county":
            # get last 3 digits as county FIPS code
            county_geoids = []
            for geoidfq in geoidfqs:
                county_geoids.append(geoidfq[-3:])
            geoidfq_str = (",").join(county_geoids)
        elif areatype_str == "place":
            # get last 5 digits for place code
            place_geoids = []
            for geoidfq in geoidfqs:
                place_geoids.append(geoidfq[-5:])
            geoidfq_str = (",").join(place_geoids)
        elif areatype_str == "zip%20code%20tabulation%20area":
            # get last 5 digits for zip code
            zctas = []
            for geoidfq in geoidfqs:
                zctas.append(geoidfq[-5:])
            geoidfq_str = (",").join(zctas)
        elif areatype_str == "tract":
            # get digits -9 thru -6 as county FIPS code (for first tract only - assumes all tracts are in same county)
            # get last 6 digits as tract code
            county_geoid = geoidfqs[0][-9:-6]
            tracts = []
            for geoidfq in geoidfqs:
                tracts.append(geoidfq[-6:])
            # return as list: tract is a special case that will be checked for in fetch_census_data_and_compute()
            geoidfq_str = [county_geoid, (",").join(tracts)]

    elif len(geoidfqs) == 1:
        if areatype_str == "county":
            # get last 3 digits as county FIPS code
            geoidfq_str = geoidfqs[0][-3:]
        elif areatype_str == "place":
            # get last 5 digits for place code
            geoidfq_str = geoidfqs[0][-5:]
        elif areatype_str == "zip%20code%20tabulation%20area":
            # get last 5 digits for zip code
            geoidfq_str = geoidfqs[0][-5:]
        elif areatype_str == "tract":
            # get digits -9 thru -6 as county FIPS code (for first tract only - assumes all tracts are in same county)
            # get last 6 digits as tract code
            county_geoid = geoidfqs[0][-9:-6]
            tract_geoid = geoidfqs[0][-6:]
            # return as list: tract is a special case that will be checked for in fetch_census_data_and_compute()
            geoidfq_str = [county_geoid, tract_geoid]
        elif areatype_str == "state":
            # return 2 digit AK FIPs code
            geoidfq_str = ["02"]
        elif areatype_str == "us":
            # return 1 digit country code
            geoidfq_str = ["1"]

    else:
        # TODO: raise an error
        print("no associated GEOIDFQs found!")

    return areatype_str, geoidfq_str


def compute_dhc(dhc_data):
    """Compute the population percentages for different age group combinations, and return the computed columns.
    This function makes a lot of assumptions about columns names; any revisions to the short names in luts.py will require changes here too.
    """

    # sex by age variables
    dhc_data["total_under_5"] = dhc_data[["m_under_5", "f_under_5"]].sum(
        axis=1, skipna=False
    )
    dhc_data["m_under_18"] = dhc_data[
        ["m_under_5", "m_5_to_9", "m_10_to_14", "m_15_to_17"]
    ].sum(axis=1, skipna=False)
    dhc_data["f_under_18"] = dhc_data[
        ["f_under_5", "f_5_to_9", "f_10_to_14", "f_15_to_17"]
    ].sum(axis=1, skipna=False)
    dhc_data["total_under_18"] = dhc_data[["m_under_18", "f_under_18"]].sum(
        axis=1, skipna=False
    )
    dhc_data["m_65_plus"] = dhc_data[
        [
            "m_65_to_66",
            "m_67_to_69",
            "m_70_to_74",
            "m_75_to_79",
            "m_80_to_84",
            "m_85_plus",
        ]
    ].sum(axis=1, skipna=False)
    dhc_data["f_65_plus"] = dhc_data[
        [
            "f_65_to_66",
            "f_67_to_69",
            "f_70_to_74",
            "f_75_to_79",
            "f_80_to_84",
            "f_85_plus",
        ]
    ].sum(axis=1, skipna=False)
    dhc_data["total_65_plus"] = dhc_data[["m_65_plus", "f_65_plus"]].sum(
        axis=1, skipna=False
    )

    # convert population counts to pcts
    dhc_data["pct_65_plus"] = round(
        dhc_data["total_65_plus"] / dhc_data["total_population"] * 100, 2
    )  # dividing NaN or by Nan will produce NaN... no need to specify
    dhc_data["pct_under_18"] = round(
        dhc_data["total_under_18"] / dhc_data["total_population"] * 100, 2
    )  # dividing NaN or by Nan will produce NaN... no need to specify
    dhc_data["pct_under_5"] = round(
        dhc_data["total_under_5"] / dhc_data["total_population"] * 100, 2
    )  # dividing NaN or by Nan will produce NaN... no need to specify

    # convert race / ethnicity counts to pcts
    dhc_data["pct_hispanic_latino"] = round(
        dhc_data["hispanic_latino"] / dhc_data["total_p9"] * 100, 2
    )
    dhc_data["pct_white"] = round(dhc_data["white"] / dhc_data["total_p9"] * 100, 2)
    dhc_data["pct_african_american"] = round(
        dhc_data["african_american"] / dhc_data["total_p9"] * 100, 2
    )
    dhc_data["pct_amer_indian_ak_native"] = round(
        dhc_data["amer_indian_ak_native"] / dhc_data["total_p9"] * 100, 2
    )
    dhc_data["pct_asian"] = round(dhc_data["asian"] / dhc_data["total_p9"] * 100, 2)
    dhc_data["pct_hawaiian_pacislander"] = round(
        dhc_data["hawaiian_pacislander"] / dhc_data["total_p9"] * 100, 2
    )
    dhc_data["pct_other"] = round(dhc_data["other"] / dhc_data["total_p9"] * 100, 2)
    dhc_data["pct_multi"] = round(dhc_data["multi"] / dhc_data["total_p9"] * 100, 2)

    return dhc_data[
        [
            "GEOID",
            "total_population",
            "pct_65_plus",
            "pct_under_18",
            "pct_under_5",
            "pct_hispanic_latino",
            "pct_white",
            "pct_african_american",
            "pct_amer_indian_ak_native",
            "pct_asian",
            "pct_hawaiian_pacislander",
            "pct_other",
            "pct_multi",
        ]
    ]


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
    var_str = (",").join(list(var_dict[survey_id]["vars"].keys()))
    areatype_str, geoidfq_str = get_census_areatype_geoid_strings(geoid_lu_df, gvv_id)

    # exclude state code from query if ZCTA
    if areatype_str == "zcta":
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&key={census_}"
    # separate list to get county and tract strings, include state FIPS code "02" for Alaska
    elif areatype_str == "tract":
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str[1]}&in=state:02&in=county:{geoidfq_str[0]}&key={census_}"
    # for statewide data, do not use geoid strings
    elif areatype_str == "state":
        url = f"{base_url}?get={var_str}&for=state:02&key={census_}"
    # for us data, do not use geoid strings
    elif areatype_str == "us":
        url = f"{base_url}?get={var_str}&for=us&key={census_}"
    # otherwise (for places and counties) include state FIPS code "02" for Alaska
    else:
        url = f"{base_url}?get={var_str}&for={areatype_str}:{geoidfq_str}&in=state:02&key={census_}"

    if print_url:
        print(f"Requesting US Census data from: {url}")

    # request the data, raise error if not returned
    with requests.get(url) as r:
        if r.status_code != 200:
            # TODO: raise error?
            print(f"No response from {survey_id} for {gvv_id}, check your URL: {url}")
        else:
            r_json = r.json()

    # convert to dataframe and reformat
    df = pd.DataFrame(r_json[1:], columns=r_json[0])

    # rename geo column depending on areatype
    if areatype_str == "tract":
        df["GEOID"] = (
            df["county"] + df["tract"]
        )  # concatenate columns to get standard 9 digit tract code
        df.drop(columns=["state", "county", "tract"], inplace=True)
    else:
        geolist = ["place", "county", "zip code tabulation area"]
        for c in df.columns:
            if c in geolist:
                df.rename(columns={c: "GEOID"}, inplace=True)
            if c == "state" and areatype_str != "state":
                df.drop(columns="state", inplace=True)
            if c == "state" and areatype_str == "state":
                df.rename(columns={c: "GEOID"}, inplace=True)
            if c == "us" and areatype_str == "us":
                df.rename(columns={c: "GEOID"}, inplace=True)

    # convert non-GEOID columns to floats, and change any negative data values to NA...
    # -6666666 is a commonly used nodata value, but there may be others. Assume all zero values and positive values are valid.
    for c in df.columns:
        if c != "GEOID":
            df[c] = df[c].astype(float)
            df[c].where(df[c] >= 0, np.nan, inplace=True)

    # use short names for variables columns if they exist in the dict
    new_cols_dict = {}
    for col in df.columns:
        try:
            new_col = var_dict[survey_id]["vars"][col]["short_name"]
            new_cols_dict[col] = new_col
        except:
            new_cols_dict[col] = col
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
    areatype_str, locationid_list = get_cdc_areatype_locationid_list(
        geoid_lu_df, gvv_id
    )

    # get base urls based on area type
    places_base_url = var_dict["cdc"]["PLACES"]["url"][areatype_str]
    sdoh_base_url = var_dict["cdc"]["SDOH"]["url"][areatype_str]

    # combine variable strings into comma separated string of strings for SoQL query
    places_var_string = (",").join(
        [f"'{x}'" for x in list(var_dict["cdc"]["PLACES"]["vars"].keys())]
    )
    sdoh_var_string = (",").join(
        [f"'{x}'" for x in list(var_dict["cdc"]["SDOH"]["vars"].keys())]
    )

    # construct SoQL query based on area type
    if areatype_str == "state":
        if use_cdc_token:
            places_url = f"{places_base_url}?$$app_token={cdc_}$where=statedesc IN ('Alaska') AND measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv')&$limit=1000000"
            sdoh_url = f"{sdoh_base_url}?$$app_token={cdc_}$where=statedesc IN ('Alaska') AND measureid IN ({sdoh_var_string})&$limit=1000000"
        else:
            places_url = f"{places_base_url}?$where=statedesc IN ('Alaska') AND measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv')&$limit=1000000"
            sdoh_url = f"{sdoh_base_url}?$where=statedesc IN ('Alaska') AND measureid IN ({sdoh_var_string})&$limit=1000000"

    elif areatype_str == "us":
        if use_cdc_token:
            places_url = f"{places_base_url}?$$app_token={cdc_}$where=measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv')&$limit=1000000"
            sdoh_url = f"{sdoh_base_url}?$$app_token={cdc_}$where=measureid IN ({sdoh_var_string})&$limit=1000000"
        else:
            places_url = f"{places_base_url}?$where=measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv')&$limit=1000000"
            sdoh_url = f"{sdoh_base_url}?$where=measureid IN ({sdoh_var_string})&$limit=1000000"

    else:
        # combine locationids into comma separated string of strings for SoQL query
        locationid_string = (",").join([f"'{x}'" for x in locationid_list])

        if use_cdc_token:
            places_url = f"{places_base_url}?$$app_token={cdc_}$where=measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv') AND locationid IN ({locationid_string})"
            sdoh_url = f"{sdoh_base_url}?$$app_token={cdc_}$where=measureid IN ({sdoh_var_string}) AND locationid IN ({locationid_string})"
        else:
            places_url = f"{places_base_url}?$where=measureid IN ({places_var_string}) AND datavaluetypeid IN ('CrdPrv') AND locationid IN ({locationid_string})"
            sdoh_url = f"{sdoh_base_url}?$where=measureid IN ({sdoh_var_string}) AND locationid IN ({locationid_string})"

    # collect separate results for PLACES and SDOH datasets
    results = []

    for url, survey in zip([places_url, sdoh_url], ["PLACES", "SDOH"]):
        if print_url:
            print(f"Requesting CDC {survey} data from: {url}")
        with requests.get(url) as r:
            if r.status_code != 200:
                print(f"No response from {survey} for {gvv_id}, check your URL: {url}")
            else:
                r_json = r.json()

        # convert to dataframe and reformat to wide
        if len(r_json) == 0 and survey == "PLACES":  # test for empty returns
            cols = [
                "pct_asthma",
                "pct_asthma_low",
                "pct_asthma_high",
                "pct_hd",
                "pct_hd_low",
                "pct_hd_high",
                "pct_copd",
                "pct_copd_low",
                "pct_copd_high",
                "pct_diabetes",
                "pct_diabetes_low",
                "pct_diabetes_high",
                "pct_mh",
                "pct_mh_low",
                "pct_mh_high",
                "pct_stroke",
                "pct_stroke_low",
                "pct_stroke_high",
                "pct_foodstamps",
                "pct_foodstamps_low",
                "pct_foodstamps_high",
                "pct_emospt",
                "pct_emospt_low",
                "pct_emospt_high",
            ]
            empty_df = pd.concat(
                [
                    pd.DataFrame(data=locationid_list, columns=["locationid"]),
                    pd.DataFrame(columns=cols),
                ]
            )
            if print_url:
                print(
                    f"Returning empty CDC PLACES dataframe for location: {locationid_list}"
                )
            results.append(empty_df)
            continue
        elif len(r_json) == 0 and survey == "SDOH":  # test for empty returns
            cols = [
                "pct_no_bband",
                "pct_no_hsdiploma",
                "pct_below_150pov",
                "pct_minority",
                "pct_crowding",
                "pct_hcost",
                "pct_single_parent",
                "pct_unemployed",
            ]
            empty_df = pd.concat(
                [
                    pd.DataFrame(data=locationid_list, columns=["locationid"]),
                    pd.DataFrame(columns=cols),
                ]
            )
            if print_url:
                print(
                    f"Returning empty CDC SDOH dataframe for location: {locationid_list}"
                )
            results.append(empty_df)
            continue
        else:
            df = pd.DataFrame(r_json)

        if survey == "PLACES":

            df.rename(
                columns={
                    "low_confidence_limit": "low",
                    "high_confidence_limit": "high",
                },
                inplace=True,
            )

            # add confidence intervals to PLACES dataframe only
            # pivot data values for measures in wide format
            df_wide = (
                df[["locationid", "measureid", "data_value"]]
                .pivot(columns=["measureid"], index="locationid", values="data_value")
                .reset_index()
                .merge(
                    df[["locationid", "totalpopulation"]].drop_duplicates(),
                    on="locationid",
                )
            )
            # pivot confidence intervals in separate dataframe and combine measureid name with confidence interval name for new column names
            df_ci = df[
                [
                    "locationid",
                    "measureid",
                    "low",
                    "high",
                ]
            ].pivot(
                columns=["measureid"],
                index="locationid",
                values=["low", "high"],
            )
            new_cols = []
            for col_1, col_0 in zip(
                df_ci.columns.get_level_values(1), df_ci.columns.get_level_values(0)
            ):
                new_cols.append(f"{col_1}_{col_0}")
            df_ci.columns = new_cols

            # merge wide data with confidence interval dataframe
            df_wide = df_wide.merge(df_ci, on="locationid")

            df_wide["totalpopulation"] = df_wide["totalpopulation"].astype(float)

        else:  # SDOH data
            df_wide = (
                df[["locationid", "measureid", "data_value"]]
                .pivot(columns=["measureid"], index="locationid", values="data_value")
                .reset_index()
                .merge(
                    df[["locationid", "totalpopulation"]].drop_duplicates(),
                    on="locationid",
                )
            )

            # pivot moe in separate dataframe and combine measureid name with moe name for new column names
            df_moe = df[
                [
                    "locationid",
                    "measureid",
                    "moe",
                ]
            ].pivot(
                columns=["measureid"],
                index="locationid",
                values=["moe"],
            )
            new_cols = []
            for col_1, col_0 in zip(
                df_moe.columns.get_level_values(1), df_moe.columns.get_level_values(0)
            ):
                new_cols.append(f"{col_1}_{col_0}")
            df_moe.columns = new_cols

            # merge wide data with confidence interval dataframe
            df_wide = df_wide.merge(df_moe, on="locationid")

            df_wide["totalpopulation"] = df_wide["totalpopulation"].astype(float)

        # change any negative data values to NA
        # nodata values are also introduced above for any empty returns
        # at the same time, rename columns using short names from var_dict or ci_dict
        for c in df_wide.columns:
            if c not in ["locationid", "totalpopulation"]:
                df_wide[c] = df_wide[c].astype(float)
                df_wide[c].where(df_wide[c] >= 0, np.nan, inplace=True)
                try:
                    short_name = var_dict["cdc"][survey]["vars"][c]["short_name"]
                except:
                    short_name = ci_dict[c]
                df_wide.rename(columns={c: short_name}, inplace=True)

        # if state or US, do the aggregation math
        if areatype_str in ["us", "state"]:
            # set standard location ids
            if areatype_str == "us":
                df_wide["locationid"] = "1"
            if areatype_str == "state":
                df_wide["locationid"] = "02"

            # compute population counts by row
            for c in df_wide.columns:
                if c not in ["locationid", "totalpopulation"]:
                    df_wide[c] = (
                        df_wide["totalpopulation"] * df_wide[c] / 100
                    )  # <<< in this temporary version of df, the columns are population counts and NOT percentages
            # groupby and sum the data columns
            agg_df = df_wide.groupby("locationid").sum()
            # then convert back to percentages
            for c in agg_df.columns:
                if c not in ["locationid", "totalpopulation"]:
                    agg_df[c] = round((agg_df[c] / agg_df["totalpopulation"] * 100), 2)
            results.append(agg_df.reset_index(drop=False))

        else:
            results.append(df_wide)

    for df in results:
        if "totalpopulation" in df.columns:
            df.drop(columns="totalpopulation", inplace=True)
            df.columns.name = ""

    out_df = reduce(lambda x, y: x.merge(y, on="locationid"), results)

    # standardize locationid to match geoids for joining later on
    # removes state FIPS for county, place, and tract; should not affect zip codes
    for idx, row in out_df.iterrows():
        if row.locationid.startswith("02") and len(row.locationid) > 2:
            out_df.loc[idx, "locationid"] = row.locationid[2:]

    return compute_cdc(out_df)

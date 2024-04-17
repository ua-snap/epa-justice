import requests
import json
import pandas as pd
from utilities.api_luts import api_key

def fetch_print_json(url, parse):
    """Helper function to use while fetching census API data. If its a top-level metadata request (parse="metadata"), 
    immediately print the JSON response. If its a geography or variable dictionary (parse="geo" or parse="var"), 
    print only the most pertinent information. In all cases, return the full JSON. Print an error message if no response.
    
    Args:
        url (str): URL to pass to requests.get()
        parse (str): code indicating how to deal with the returned JSON
    Returns:
        JSON response from URL
    """
    with requests.get(url) as r:
        if r.status_code != 200:
            print("No response, check your URL")
        else:
            r_json = r.json()

            if parse=="geo":
                for name in r_json["fips"]:
                    #some geographies require additional geographies during a query, so we test "requires" in the dict and print if it exists
                    try:
                        print(f"NAME: {name['name']}: REQUIRES: {name['requires']}")
                    except:
                        print(f"NAME: {name['name']}")
                return r_json
            
            
            elif parse=="var":
                for key in r_json['variables'].keys():
                    print(f"{key}: {r_json['variables'][key]['label']}")
                return r_json
            
            elif parse=="metadata":
                r_json_to_print = json.dumps(r_json, indent=2)
                print(r_json_to_print)
                return r_json
            
            else:
                print("Please include a parse argument.")


def fetch_vars_by_zip_to_df(base, vars, var_dict, zips):
    """Helper function to use while fetching census API data. Joins a base URL to a list of variable codes and zip code tabulation areas,
    and requests the URL. Returns the JSON response. Print an error message if no response.
    
    Args:
        base (str): base URL to query census API
        vars (list): list of variable code strings
        zips (list): list of zip code strings
        var_dict (dict): dataset variable dictionary from api_luts.py
    Returns:
        reformatted pandas.DataFrame
    """
    url = str(base + "?get=" + (',').join(vars) + "&for=zip%20code%20tabulation%20area:" + (',').join(zips) + "&key=" + api_key)
    
    print(f"fetching data from: {url}")

    with requests.get(url) as r:
        if r.status_code != 200:
            print("No response, check your URL")
        else:
            r_json = r.json()

    cols = []
    for col in r_json[0]:
        if col=="zip code tabulation area":
            cols.append("ZCTA")
        else:
            try:
                cols.append(var_dict[col]["short_name"])
            except:
                print("Variable not found in dataset variable dictionary!")
                return None

    df = pd.DataFrame(r_json[1:], columns=cols)
    #reorganize columns so that ZCTA is first
    new_cols = df.columns.tolist()
    new_cols.remove('ZCTA')
    new_cols.insert(0, 'ZCTA')
    df = df[new_cols]

    #convert from string to numeric, but skip the first row (ZCTA) since its more a category than a number
    df.iloc[:,1:] = df.iloc[:,1:].apply(pd.to_numeric)

    return df

# epa-justice
Accessing US Census data and CDC health data via API to support demographic data summaries in SNAP's [Northern Climate Reports](https://northernclimatereports.org/).

This repo demonstrates access to the US Census Data via their [API](https://www.census.gov/data/developers/data-sets.html), and the CDC [PLACES](https://www.cdc.gov/places/index.html) dataset via their [API](https://dev.socrata.com/foundry/data.cdc.gov/swc5-untb).

It includes some basic helper functions and lookup tables to support the EPA-Justice-HIA project which may pull demographic data from the US Census and CDC datasets. The functions here will take Alaska community IDs (from [this collection](https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/point/alaska_point_locations.csv)) as their input, and produce tables of data relevant to the EPA-Justice-HIA project. These functions should be incorporated into NCR at some point.

This repo also archives some exploratory work on this project.

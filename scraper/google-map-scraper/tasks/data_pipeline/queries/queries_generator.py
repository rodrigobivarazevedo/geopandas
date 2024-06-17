import geopandas as gpd
import pandas as pd
import numpy as np


filename = "gadm36_AUT_2.shp"
letters = filename.split("_")[1]
regions_level2 = gpd.read_file(f"../country_boundaries/gadm36_AUT_shp/{filename}")

# get regions names for each state
regions_level2 = regions_level2[['NAME_1', 'NAME_2', 'geometry']]
regions_level2.columns = ['state', 'region', 'geometry']

# create a list with the names of the regions for a each state
states = regions_level2.groupby('state')['region'].apply(list).reset_index()



# generate search queries for all types of farms in each regions of each state
farm_types = [
    "dairy farms",
    "poultry farms",
    "cattle farms",
    "livestock farms",
    "pig farms",
    "fish farms",
    "aquaculture farms",
    "egg farmers",
    "chicken hatchery",
    "shrimp farms",
    "seafood farms",
    "beef farms",
    "meat producer"
]

queries = []

for state in states['state']:
    for farm_type in farm_types:
        for region in states[states['state'] == state]['region'].values[0]:
        #for farm_type in farm_types:
            queries.append(f'{farm_type} in {region}, {state}')
            


#store the list in a pandas dataframe
queries_df = pd.DataFrame(queries, columns=['query'])
# divide the dataframe into each state based on the list of states
queries_df2 = queries_df.copy()
queries_df2['state'] = queries_df['query'].apply(lambda x: x.split(', ')[1])

queries_df3 = queries_df2.copy()
queries_df3.drop(columns=['state']).to_csv(f'farm_queries_{letters}.csv', index=False)
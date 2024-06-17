import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

regions_level2 = gpd.read_file("../country_boundaries/gadm41_DEU_shp/gadm41_DEU_2.shp")
# get regions names for each state
regions_level2 = regions_level2[['NAME_1', 'NAME_2', 'geometry']]
regions_level2.columns = ['state', 'region', 'geometry']
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

lab_types = [
    
                'diagnostic center',"microbiology lab","clinical microbiology", "veterinary microbiology",
                "veterinary diagnostic lab", 'veterinary laboratory', 
    ]

queries = []

def get_farm_queries(states, farm_types):
    queries = []
    for state in states['state']:
        for region in states[states['state'] == state]['region'].values[0]:
            for farm_type in farm_types:
                queries.append(f'{farm_type} in {region}, {state}')
    return queries

def get_lab_queries(states, lab_types):
    queries = []
    for state in states['state']:
        for region in states[states['state'] == state]['region'].values[0]:
            for lab_type in lab_types:
                queries.append(f'{lab_type} in {region}, {state}')
    return queries


queries = get_lab_queries(states, lab_types)
# store the list in a pandas dataframe
queries_df = pd.DataFrame(queries, columns=['query'])
queries_df.to_csv('lab_queries_germany2.csv', index=False)

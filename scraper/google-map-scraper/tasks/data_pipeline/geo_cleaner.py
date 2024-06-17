import logging
import pandas as pd
import numpy as np  
import geopandas as gpd
from io import StringIO


class GeoCleaner:
    def __init__(self, file):
        self # Capture the output of df.info()
        self.info_buffer = StringIO()
        self.file = file
        self.df = None
        self.regions_level2 = gpd.read_file("../france_boundary/gadm36_FRA_2.shp")
        self.logger = self.setup_logger()
        self.translations = {
            'Farm': 'farm',
            'Dairy farm': 'dairy_farm',
            'Poultry farm': 'poultry_farm',
            'Organic farm': 'organic_farm',
            'Cattle breeder': 'cattle_breeder',
            "Cattle farm" : "cattle_farm",
            'Livestock farm': 'livestock_farm',
            'Pig farm': 'pig_farm',
            'Fish farm': 'fish_farm',
            'Aquaculture farm': 'aquaculture_farm',
            'Chicken hatchery' : 'poultry_farm',
            'Egg farmer': 'egg_farmer',
            'Shrimp farm': 'shrimp_farm',
            'Seafood farm' : 'aquaculture_farm',
            'Farmer': 'farmer',
            "Dairy" : "dairy_farm",
            "Livestock breeder": "livestock_breeder",
            'Livestock': 'livestock_breeder',
            "Livestock producer": "livestock_breeder",
            "Meat Producer": "meat_producer",
        }
 
    def setup_logger(self):
        logger = logging.getLogger('data_cleaner')
        logger.setLevel(logging.DEBUG)  # Default logging level
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Add console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        # Add file handler
        # Add file handler with timestamp in the file name
        #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f"data_cleaner.log"
        fh = logging.FileHandler(log_file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
        return logger

    
    def load_data(self):
        self.logger.info("Loading data...")
        self.df = pd.read_csv(self.file)
        self.logger.info(f"Data loaded successfully from {self.file}")

    def check_data(self):
        self.logger.info("Checking data...")
        self.logger.info(f"Unique main categories:\n{self.df['main_category'].unique()}")
        self.logger.info(f"Main categories distributions:\n{self.df['main_category'].value_counts()}")
        # Capture the output of df.info()
        self.info_buffer = StringIO()
        self.df.info(buf=self.info_buffer)
        info_string = self.info_buffer.getvalue()
        
    
        # Log the captured information
        self.logger.info(f"Info:\n{info_string}")
        #self.logger.info(f"Info:\n{self.df.info()}")
        self.logger.info("Data check completed.")
        
    def remove_unwanted_columns(self):
        initial_columns = len(self.df.columns)
        self.logger.info("Removing unwanted columns...")
        #columns_remove = ["place_id", "description","is_spending_on_ads","reviews","competitors","website","can_claim","emails","phones","about","images","hours","most_popular_times","popular_times","menu","reservations","order_online_links","featured_reviews","detailed_reviews","detailed_address",
           #             "linkedin","twitter","facebook","youtube","instagram","pinterest","github","snapchat","tiktok","price_range","reviews_per_rating","featured_question","reviews_link","cid","owner","featured_image","rating","workday_timing","closed_on","phone","review_keywords","status","time_zone","data_id","plus_code"]
        #self.df = self.df.drop(columns=columns_remove)
        # keep only the columns we need
        #self.df = self.df.loc[:, ["name", "competitors", "website", "main_category", "categories", "phone", "address", "coordinates","link"]].copy()
        desired_columns = ["name", "website", "main_category", "categories", "phone", "address", "coordinates","link"]
        self.df = self.df[desired_columns]
        self.logger.info("Unwanted columns removed.")
        removed_columns = initial_columns - len(self.df.columns)
        self.logger.info(f"Removed {removed_columns} columns")

    def remove_null_and_duplicates(self):
        initial_rows = len(self.df)
        self.logger.info("Removing null values and duplicates...")
        self.df = self.df.dropna(subset=['coordinates'])
        self.df = self.df.drop_duplicates(subset=['coordinates'])
        self.logger.info("Null values and duplicates removed.")
        removed_rows = initial_rows - len(self.df)
        self.logger.info(f"Removed {removed_rows} rows with null or duplicate values")
    
    def format_main_categories(self):
        self.logger.info("Formating main categories...")
        self.df[['latitude','longitude']] = self.df['coordinates'].str.split(',', expand=True)
        self.df['main_category'] = self.df['main_category'].map(self.translations).fillna(self.df['main_category'])
        self.logger.info("Main categories formated.")

    def format_categories(self):
        self.logger.info("Formating categories...")
        def translate_categories(category_list):
            translated_list = []
            if isinstance(category_list, float) and np.isnan(category_list):
                return translated_list
            for category in category_list.split(", "):
                if category in self.translations:
                    translated_list.append(self.translations[category])
                else:
                    translated_list.append(category)
            return translated_list
        self.df['categories'] = self.df['categories'].apply(translate_categories)
        self.logger.info("Categories formated.")
    
    def keep_specific_categories(self):
        self.logger.info("Keeping specific categories...")
        main_categories_keywords = {
            'farm',
            'dairy_farm',
            'poultry_farm',
            'organic_farm',
            'cattle_breeder',
            "cattle_farm",
            'livestock_farm',
            'pig_farm',
            'fish_farm',
            'aquaculture_farm',
            'egg_farmer',
            'shrimp_farm',
            'farmer',
            "dairy",
            "livestock_breeder",
            "meat_producer",
            }
        keywords = {
            'dairy_farm',
            'poultry_farm',
            'cattle_breeder',
            "cattle_farm",
            'livestock_farm',
            'pig_farm',
            'fish_farm',
            'aquaculture_farm',
            'egg_farmer',
            'shrimp_farm',
            "dairy",
            "livestock_breeder",
            "meat_producer",
        }
            
        # Filter rows based on main_category column
        self.logger.info("Filtering rows based on main_category column...")
        self.df = self.df[self.df['main_category'].isin(main_categories_keywords)]
        self.logger.info("Rows kept based on main_category column.")
        self.check_data()
        self.logger.info("Checking for keywords in categories list...")
        def check_keywords(category_list):
            if isinstance(category_list, float) and np.isnan(category_list):
                pass
            for category in category_list:
                if category in keywords:
                    return True
            return False
        self.df['contains_keyword'] = self.df['categories'].apply(check_keywords)
        self.df = self.df[self.df['contains_keyword']]
        self.logger.info("Keywords checked.")
        self.check_data()
        self.df.drop(columns=['contains_keyword'], inplace=True)
        self.df = self.df.reset_index(drop=True)
        self.logger.info("Specific categories kept.")
        
        
        self.logger.info("Filtering categories...")
        def filter_categories(category_list):
            if isinstance(category_list, float) and np.isnan(category_list):
                return []
            return [category for category in category_list if category in keywords]

        self.df['categories'] = self.df['categories'].apply(filter_categories)
        self.logger.info("Categories filtered.")
        
        
        keywords2 = {
            'dairy_farm',
            'poultry_farm',
            'cattle_breeder',
            "cattle_farm",
            'pig_farm',
            'fish_farm',
            'aquaculture_farm',
            'egg_farmer',
            'shrimp_farm',
            "dairy",
        }

        self.logger.info("Updating main categories...")
        def update_main_category(row):
            if row['main_category'] not in keywords2:
                if len(row['categories']) == 1:
                    if row['categories'][0] in keywords2:
                        return row['categories'][0]
                    else:
                        return "multiple"
                else:
                    return "multiple"
            return row['main_category']

        self.df['main_category'] = self.df.apply(update_main_category, axis=1)
        self.logger.info("Main categories updated.")
        # check values counts for categories column where main category is multiple
        self.logger.info(f"Farms with multiple as main category Categories count:\n{self.df[self.df['main_category'] == 'multiple']['categories'].value_counts()}")     
           
    def get_animal_types(self):
        self.logger.info("Getting animal type...")
        def get_animal_type(row):
            category_to_animal = {
            'dairy_farm' : 'cows',
            'poultry_farm' : 'poultry',
            'cattle_breeder' : 'cows',
            "cattle_farm" : "cows",
            'livestock_farm' : 'other',
            'pig_farm' : 'pigs',
            'fish_farm' : 'fish',
            'aquaculture_farm' : 'fish',
            'egg_farmer' : 'poultry',
            'shrimp_farm' : 'fish',
            "dairy" : "cows",
            "livestock_breeder" : "other",
            "meat_producer" : "other",
        }
            return category_to_animal.get(row['main_category'], 'other')
        
        self.df['animal_type'] = self.df.apply(get_animal_type, axis=1)
        self.logger.info(f"Animal type count:\n{self.df.animal_type.value_counts()}")
        self.logger.info("Animal type added.")
        
    def transform_gpd(self):
        self.logger.info("Converting the DataFrame to a GeoDataFrame...")
         # Convert the DataFrame to a GeoDataFrame
        self.df = gpd.GeoDataFrame(self.df, geometry=gpd.points_from_xy(self.df.longitude, self.df.latitude))
        # Set the coordinate reference system (CRS) to EPSG 4326
        self.df.crs = {'init': 'epsg:4326'}
        self.logger.info("GeoDataFrame created with CRS: 4326")
        
    def assign_region(self):
        self.logger.info("Assigning region and state to farms...")
        # Perform a spatial join to assign each farm to a department
        joined_gdf = gpd.sjoin(self.df, self.regions_level2, how="left", op="within")
        # Aggregate farms by state and count the number of farms in each state
        # farms_by_state = joined_gdf.groupby('NAME_1')['name'].count()
        # farms_in_bretagne = joined_gdf[joined_gdf['NAME_1'] == 'Bretagne']

        # Aggregate farms by department and count the number of farms in each department
        #farms_by_department = joined_gdf.groupby('NAME_2')['name'].count()
        self.logger.info(f"Farms by department:\n{joined_gdf.NAME_2.value_counts()}")
        # Select only the desired columns (state and department names)
        desired_columns = ['NAME_0','NAME_1', 'NAME_2']
        joined_gdf_filtered = joined_gdf[desired_columns]
        
        # Merge the filtered spatial join result with the original GeoDataFrame
        joined_gdf_final = self.df.merge(joined_gdf_filtered, left_index=True, right_index=True)
        # change the column names to state and department
        joined_gdf_final.rename(columns={'NAME_0': 'country', 'NAME_1': 'state', 'NAME_2': 'department'}, inplace=True)
        self.logger.info(f"Farms by state:\n{joined_gdf_final.state.value_counts()}")

        # Filter farms by state
        #farms_in_bretagne = joined_gdf_final[joined_gdf_final['state'] == "Île-de-France"]
        #self.logger.info(f"Farms by selected state:\n{farms_in_bretagne.state.value_counts()}")
        #self.logger.info(f"Farms by departments in selected state:\n{farms_in_bretagne.department.value_counts()}")
        #self.df = farms_in_bretagne
        self.df = joined_gdf_final
        
    def optimize_performance(self):
        # Perform performance optimization here
        # For example, using pandas' built-in functions like apply or map instead of looping over rows
        # use paralell processing
        pass
    
    def data_validation(self):
        # Implement data validation checks here
        # For example, validate data types, check for outliers, or enforce constraints
        pass
    
    def clean(self):
        self.load_data()
        self.remove_unwanted_columns()
        self.remove_null_and_duplicates()
        self.format_main_categories()
        self.check_data()
        self.format_categories()
        self.keep_specific_categories()
        self.check_data()
        self.get_animal_types()
        self.check_data()
        self.transform_gpd()
        self.check_data()
        self.assign_region()
        self.check_data()
        
        
        self.df.to_csv(f"{self.file.replace('.csv', '_cleaned.csv')}", index=False)
        self.logger.info(f"Cleaning process completed. Cleaned data saved to {self.file.replace('.csv', '_cleaned.csv')}")

#cleaner = GeoCleaner("/Users/rodrigoazevedo/repos/geopandas/data/Île-de-France.csv")
#cleaner.clean()



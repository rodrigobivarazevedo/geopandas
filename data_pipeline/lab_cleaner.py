import logging
import pandas as pd
import numpy as np  
import geopandas as gpd
from io import StringIO
import matplotlib.pyplot as plt

class GeoCleaner:
    def __init__(self, file):
        self.info_buffer = StringIO()
        self.file = file
        self.df = None
        self.regions_level2 = gpd.read_file("../country_boundaries/gadm41_AUT_shp/gadm41_AUT_2.shp")
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger('data_cleaner')
        logger.setLevel(logging.DEBUG)  # Default logging level
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Add console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Add file handler
        log_file_name = f"data_cleaner.log"
        fh = logging.FileHandler(log_file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    def load_data(self):
        self.logger.info("Loading data...")
        try:
            self.df = pd.read_csv(self.file)
            self.logger.info(f"Data loaded successfully from {self.file}")
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")

    def check_data(self):
        self.logger.info("Checking data...")
        try:
            self.logger.info(f"Unique main categories:\n{self.df['main_category'].unique()}")
            self.logger.info(f"Main categories distributions:\n{self.df['main_category'].value_counts()}")
            # Capture the output of df.info()
            self.info_buffer = StringIO()
            self.df.info(buf=self.info_buffer)
            info_string = self.info_buffer.getvalue()
            # Log the captured information
            self.logger.info(f"Info:\n{info_string}")
            self.logger.info("Data check completed.")
        except Exception as e:
            self.logger.error(f"Error checking data: {str(e)}")

    def remove_unwanted_columns(self):
        initial_columns = len(self.df.columns)
        self.logger.info("Removing unwanted columns...")
        try:
            desired_columns = ["name", "website", "main_category", "categories", "phone", "address", "coordinates", "link"]
            self.df = self.df[desired_columns]
            self.logger.info("Unwanted columns removed.")
            removed_columns = initial_columns - len(self.df.columns)
            self.logger.info(f"Removed {removed_columns} columns")
        except Exception as e:
            self.logger.error(f"Error removing unwanted columns: {str(e)}")

    def remove_null_and_duplicates(self):
        self.logger.info("Removing null values and duplicates...")
        try:
            initial_rows = len(self.df)
            self.df.dropna(subset=["coordinates"], inplace=True)
            removed_rows = initial_rows - len(self.df)
            self.logger.info(f"Removed {removed_rows} rows with null values")
            self.df.drop_duplicates(subset=['coordinates'], inplace=True)
            final_rows = len(self.df)
            self.logger.info("Null values and duplicates removed.")
            removed_rows = initial_rows - final_rows
            self.logger.info(f"Removed {removed_rows} rows with duplicate values")
        except Exception as e:
            self.logger.error(f"Error removing null values and duplicates: {str(e)}")

    def format_main_categories(self):
        self.logger.info("Formatting main categories...")
        try:
            self.df[['latitude', 'longitude']] = self.df['coordinates'].str.split(',', expand=True)
            self.df['main_category'] = self.df['main_category'].str.lower().str.strip().str.replace(" ", "_", regex=False)
            self.logger.info("Main categories formatted.")
        except Exception as e:
            self.logger.error(f"Error formatting main categories: {str(e)}")

    def format_categories(self):
        self.logger.info("Formatting categories...")
        try:
            def translate_categories(category_list):
                translated_list = []
                if isinstance(category_list, float) and np.isnan(category_list):
                    return translated_list
                for category in category_list.split(", "):
                    translated_list.append(category.lower().strip().replace(" ", "_"))
                return translated_list

            self.df['categories'] = self.df['categories'].apply(translate_categories)
            self.logger.info("Categories formatted.")
            self.logger.info(f"Categories distributions:\n{self.df['categories'].value_counts()}")
        except Exception as e:
            self.logger.error(f"Error formatting categories: {str(e)}")

    def keep_specific_categories(self):
        self.logger.info("Keeping specific categories...")
        try:
            main_categories_keywords = {
                "diagnostic_lab","laboratory",
                "testing_lab", "diagnostic_testing_lab", "s2_diagnostics_lab",
                "s2_diagnostic_facility","pharmaceutical_lab",'dialysis_center',
                'diagnostic_center',"microbiology","clinical_microbiology", "veterinary_microbiology",
                "veterinary_diagnostic", 'medical_laboratory', "biochemistry_lab"
            }
            keywords = {
                "diagnostic_lab","pharmaceutical_lab","biochemistry_lab",'dialysis_center',
                "testing_lab", "diagnostic_testing_lab", "s2_diagnostics_lab",
                "s2_diagnostic_facility",
                'diagnostic_center',"microbiology","clinical_microbiology", "veterinary_microbiology",
                "veterinary_diagnostic", 'medical_laboratory', "laboratory"
            }
            keywords2 = {'diagnostic_center'}

            self.df = self.df[self.df['main_category'].isin(main_categories_keywords)]
            self.logger.info("Rows kept based on main_category column.")

            def check_keywords(category_list):
                if isinstance(category_list, list):
                    return any(category in keywords for category in category_list)
                return False

            self.df['contains_keyword'] = self.df['categories'].apply(check_keywords)
            self.df = self.df[self.df['contains_keyword']]
            self.logger.info("Keywords checked.")

            self.df.drop(columns=['contains_keyword'], inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            self.logger.info("Specific categories kept.")
            self.logger.info(f"Categories distributions:\n{self.df['categories'].value_counts()}")

            def filter_categories(category_list):
                if isinstance(category_list, list):
                    return [category for category in category_list if category in keywords]
                return []

            self.df['categories'] = self.df['categories'].apply(filter_categories)
            self.logger.info("Categories filtered.")

            def update_main_category(row):
                if row['main_category'] not in keywords:
                    if len(row['categories']) == 1:
                        return row['categories'][0]
                    else:
                        return "multiple"
                return row['main_category']

            self.df['main_category'] = self.df.apply(update_main_category, axis=1)
            self.logger.info("Main categories updated.")
        except Exception as e:
            self.logger.error(f"Error keeping specific categories: {str(e)}")

    def transform_gpd(self):
        self.logger.info("Converting the DataFrame to a GeoDataFrame...")
        try:
            self.df['latitude'] = self.df['latitude'].replace('None', np.nan).astype(float)
            self.df['longitude'] = self.df['longitude'].replace('None', np.nan).astype(float)
            self.df.dropna(subset=['longitude'], inplace=True)
            self.df = gpd.GeoDataFrame(self.df, geometry=gpd.points_from_xy(self.df.longitude, self.df.latitude))
            self.df.crs = {'init': 'epsg:4326'}
            self.logger.info("GeoDataFrame created with CRS: 4326")
        except Exception as e:
            self.logger.error(f"Error converting DataFrame to GeoDataFrame: {str(e)}")

    def assign_region(self):
        self.logger.info("Assigning region and state to farms...")
        try:
            joined_gdf = gpd.sjoin(self.df, self.regions_level2, how="left", op="within")
            joined_gdf = joined_gdf.dropna(subset=['COUNTRY'])
            desired_columns = ['COUNTRY', 'NAME_1', 'NAME_2']
            joined_gdf_filtered = joined_gdf[desired_columns]
            joined_gdf_final = self.df.merge(joined_gdf_filtered, left_index=True, right_index=True)
            joined_gdf_final.rename(columns={'COUNTRY': 'country', 'NAME_1': 'state', 'NAME_2': 'department'}, inplace=True)
            self.logger.info(f"Farms by department:\n{joined_gdf_final.department.value_counts()}")
            self.logger.info(f"Farms by state:\n{joined_gdf_final.state.value_counts()}")
            self.df = joined_gdf_final
        except Exception as e:
            self.logger.error(f"Error assigning region and state: {str(e)}")

    def data_validation(self):
        try:
            null_columns = self.df.columns[self.df.isnull().any()]
            if not null_columns.empty:
                self.logger.info(f"Null values found in columns: {', '.join(null_columns)}")
        except Exception as e:
            self.logger.error(f"Error checking for null values: {str(e)}")

        try:
            numerical_columns = self.df.select_dtypes(include=np.number).columns
            for column in numerical_columns:
                outliers = self.find_outliers(self.df[column])
                if outliers:
                    self.logger.info(f"Outliers found in column {column}: {outliers}")
        except Exception as e:
            self.logger.error(f"Error checking for outliers: {str(e)}")

    def clean(self):
        try:
            self.load_data()
            self.remove_unwanted_columns()
            self.remove_null_and_duplicates()
            self.format_main_categories()
            self.check_data()
            self.format_categories()
            self.keep_specific_categories()
            self.check_data()
            self.check_data()
            self.transform_gpd()
            self.check_data()
            self.assign_region()
            self.check_data()
            self.check_data()
            self.data_validation()
            self.df.to_csv(f"processed_data_lab/austria_labs_cleaned2.csv", index=False)
            self.logger.info(f"Cleaning process completed. Cleaned data saved to {self.file.replace('.csv', '_cleaned.csv')}")
        except Exception as e:
            self.logger.error(f"Error in cleaning process: {str(e)}")



cleaner = GeoCleaner("/Users/rodrigoazevedo/repos/geopandas/data/lab_raw_csv/labs_austrial.csv")
cleaner.clean()

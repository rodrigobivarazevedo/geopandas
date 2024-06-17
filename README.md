# geopandas

## code for cleaning and formatting in data pipeline directory 

### Usage of the GeoCleaner Class 

- For customized usage feel free to adapt the class methods to your needs
- This code is highly specialized for spatial data about farms all over Europe
- The code formats, cleans and keeps only data about farms and also performs spatial joins on data to specifically keep only farms in desired country

- Initialize the class with the desired csv file
cleaner = GeoCleaner("/data/portugal.csv")

- depending on the contry you wish to perform spatial analysis, make sure to get the countries boundaries at: https://gadm.org/download_country.html

- after downloading the file, update the Geocleaner class and add the shapefile
self.regions_level2 = gpd.read_file("../country_boundaries/gadm41_PRT_shp/gadm41_PRT_2.shp")

- depending on desired operations use the class methods freely:
load_data()
self.remove_unwanted_columns()
self.remove_null_and_duplicates()
self.format_main_categories()
self.check_data()
self.format_categories()
self.keep_specific_categories()
self.get_animal_types()
self.transform_gpd()
self.assign_region()
self.data_validation()
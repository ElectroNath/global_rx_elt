import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
# from varname import nameof #used for getting the name of a variable as a string

#load the dotenv file
load_dotenv(override=True)


# Access the variables
database_url = os.getenv('PG_URL')

#function to Read in the excel workbook
def load_excel_data(path:str, sheetname:str)-> pd.DataFrame:
    '''
    Function to load Excel sheets into pandas dataframe
    path:- The location of the excel file to be read e.g. /path/file.xlsx 
    sheetname:- The name of the sheet in the excel workbook usually found on the Tab bar of the excel workbook
    Returns a Dataframe containing the information of the sheet specified
    '''
    print(f'\nLoading data from {path}...')
    data = pd.read_excel(path, sheet_name=sheetname)
    return data


# Basic Transformation function for column names
def transform_column_names(df:pd.DataFrame):
    '''
    This function transforms the column names by replacing the spaces by _ and removing all leading or lagging space
    '''
    print('\nTransforming Column names')
    df.columns = [column.strip().lower().replace(' ', '_').replace('-', '_') for column in df.columns]
    return df


#Normalization --> Breaking it all apart #Orders #Products #Customers #location
def normalize_data(df:pd.DataFrame):
    '''
    Function to mormalize loaded data into four tables
    '''
    
    print(f'\nExtracting & Transforming products... current length = {len(df)}')

    products_data = df[['product_id', 'category', 'sub_category', 'product_name']].drop_duplicates()
    print(f'Products Extraction & transformation completed... with {products_data.shape[0]} samples')

    customers_data = df[['customer_id', 'customer_name']].drop_duplicates()
    print(f'Customers Extraction & transformation completed... with {customers_data.shape[0]} samples')


    locations_data = df[['city', 'state', 'country']].drop_duplicates()
    print(f'Locations Extraction & transformation completed... with {locations_data.shape[0]} samples')

    #Validate order duplicates
    orders_data = df[['row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode','customer_id', 'product_id', 'segment','postal_code',
                        'market', 'region', 'sales', 'quantity', 'discount','profit', 'shipping_cost','order_priority']]
    print(f'Orders Extraction & transformation completed... with {orders_data.shape[0]} samples')
    
    return {'products':products_data, 'customers':customers_data, 'locations':locations_data, 'orders':orders_data}


# write dataframes to csv files for intermiediate storage
def write_to_csv(df:pd.DataFrame, name_of_file:str):
    '''
    Function to write pandas dataframe into a csv file
    df: dataframe to be converted
    name_of_file: proposed name for the csv file
    '''
    print(f'\n>>> writing {name_of_file} data to intermiediate storage....')
    return df.to_csv(f'outputs/models/{name_of_file}.csv', index=False)


def load_to_database(path_to_csvs:str):
    '''
    Funtion to read files from intermediate storage then upload to datawarehouse
    '''
    engine = create_engine(database_url)
    connection = engine.connect()
    
    file_names = os.listdir(path_to_csvs)
    
    for file in file_names:
        table_name = file.split('.')[0]
        each_data = pd.read_csv(f'{path_to_csvs}/{file}')
        each_data.to_sql(table_name, con=connection, if_exists='replace', index=False)
        print('\nData loaded to {table_name} table in the datawarehouse')
    connection.close()
    
    



if __name__ == '__main__':    
    # calling the function to laod the excel file
    path = "data/global-superstore-data.xlsx"
    data = load_excel_data(path, "Orders")

    # calling function to create a new data frame call tr_data
    tr_data = transform_column_names(data)
    
    # Normilizing the data
    norm_data = normalize_data(tr_data)
    
    # write files to csv
    for data in norm_data.items():
        write_to_csv(data[1], data[0])
        print(f'{data[0]} file has been created and stored')
    print(f'{len(norm_data)} files Written Succefully\n\n')
    
    # Load files to Datawarehouse
    load_to_database('outputs/models')
    
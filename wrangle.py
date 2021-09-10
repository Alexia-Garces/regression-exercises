import pandas as pd
import numpy as np
import os
from env import host, user, password
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer

###################### Acquire Zillow Data ######################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    It takes in a string name of a database as an argument.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
    
    
def new_zillow_data():
    '''
    This function reads the zillow data from the Codeup db into a df,
    write it to a csv file, and returns the df.
    '''
    # Create SQL query.
    sql_query = """
    SELECT 
        bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, 
        taxvaluedollarcnt, yearbuilt, taxamount, fips
    FROM properties_2017
    WHERE propertylandusetypeid = 261;
    """

    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query, get_connection('zillow'))
    
    #rename columns
    df = df.rename(columns = {'bedroomcnt': 'bedrooms', 
                           'bathroomcnt':'bathrooms',
                           'calculatedfinishedsquarefeet': 'square_feet',
                           'taxvaluedollarcnt':'tax_value',
                           'yearbuilt':'year_built'})
    
    return df



def get_zillow_data():
    '''
    This function reads in zillow data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('zillow_df.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv('zillow_df.csv', index_col=0)
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = new_zillow_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('zillow_df.csv')
        
    return df

###################### Prepare Zillow Data ######################
def split_continuous(df):
    '''
    Takes in a df
    Returns train, validate, and test DataFrames
    '''
    # Create train_validate and test datasets
    train_validate, test = train_test_split(df, 
                                        test_size=.2, 
                                        random_state=123)
    # Create train and validate datsets
    train, validate = train_test_split(train_validate, 
                                   test_size=.3, 
                                   random_state=123)

    # Take a look at your split datasets

    print(f'train -> {train.shape}')
    print(f'validate -> {validate.shape}')
    print(f'test -> {test.shape}')
    return train, validate, test

def prepare_zillow(df):
    
    """
    This function takes in the zillow dataframe and retuns the cleaned and prepped dataset
    to use when doing exploratory data analysis
    """
    #remove outliers
    df = remove_outliers(df, 1.7, ['bedrooms', 'bathrooms', 'square_feet', 'tax_value', 'taxamount'])
    
    #convert column data types
    df.fips = df.fips.astype(object)
    df.year_built = df.year_built.astype(object)
    
    #drop taxamount column
    df = df.drop(columns=['taxamount'])
    
    #split data
    split_continuious(df)
    
    #impute year_built with mode
    #create imputer
    imputer = SimpleImputer(strategy='most_frequent')
    #fit to train
    imputer.fit(train[['year_built']])
    #transform data
    train[['year_built']] = imputer.transform(train[['year_built']])
    validate[['year_built']] = imputer.transform(validate[['year_built']])
    test[['year_built']] = imputer.transform(test[['year_built']])
    
    return train, validate, test

def wrangle_zillow():
    """
    This functions acquires the zillow data and retuns the cleaned and prepped dataframe
    to use when doing exploratory data analysis
    """
    train, validate, test = prepare_zillow(get_zillow_data(new_zillow_data()))
    return train, validate, test
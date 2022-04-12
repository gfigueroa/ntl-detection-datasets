from pandas import DataFrame, read_csv

import pandas as pd
import numpy as np
import os.path

ENVIRONMENT = "windows"  # values: ["windows", "nix"]
DATA_VERSION = 3  # values: [1, 2, ...]
EXPERIMENTS_VERSION = 2.0

preprocessing_dir = ''  # root folder of datasets
if ENVIRONMENT == "windows":
    separator = '\\'
else:
    separator = '/'

intermediate_dir = preprocessing_dir + "v" + str(EXPERIMENTS_VERSION) + separator + "1. Intermediate" + separator
filtered_dir = preprocessing_dir + "v" + str(EXPERIMENTS_VERSION) + separator + "2. Filtered" + separator

datos_hurto_file = intermediate_dir + 'datos_hurto'
datos_no_hurto_file = intermediate_dir + 'datos_no_hurto'
filtered_hurto_file = filtered_dir + "filtered_hurto"
filtered_no_hurto_file = filtered_dir + "filtered_no_hurto"
if DATA_VERSION > 1:
    # Check that current data version exists
    if os.path.isfile(datos_hurto_file + '_v{0}'.format(DATA_VERSION)):
        datos_hurto_file += '_v{0}'.format(DATA_VERSION)
        datos_no_hurto_file += '_v{0}'.format(DATA_VERSION)
    else:
        datos_hurto_file += '_v{0}'.format(DATA_VERSION - 1)
        datos_no_hurto_file += '_v{0}'.format(DATA_VERSION - 1)
    filtered_hurto_file += '_v{0}'.format(DATA_VERSION)
    filtered_no_hurto_file += '_v{0}'.format(DATA_VERSION)

# Always add .csv as extension
datos_hurto_file += '.csv'
datos_no_hurto_file += '.csv'
filtered_hurto_file += '.csv'
filtered_no_hurto_file += '.csv'

print "Starting Data Filtering process..."

print datos_hurto_file
print datos_no_hurto_file
print filtered_hurto_file
print filtered_no_hurto_file


# In[2]:

# Load preprocessed files

print "Loading preprocessed files..."

hurtos_df = pd.read_csv(datos_hurto_file)

no_hurtos_df = pd.read_csv(datos_no_hurto_file)

hurtos_df.head()


# In[4]:

# Convert dates

print "Converting dates..."

hurtos_df['FILE_DATE'] = pd.to_datetime(hurtos_df['FILE_DATE'])
no_hurtos_df['FILE_DATE'] = pd.to_datetime(no_hurtos_df['FILE_DATE'])

hurtos_df['FILE_DATE'].head()


# In[7]:

# Remove months from customer when customer has less than N days of data
MIN_DAYS = 10 # Arbitrary value (at least 10 days worth of data)

def filter_missing_days(df):

    # For each client
    to_remove = []
    medidores = df['MEDIDOR'].unique()
    for medidor in medidores:
        #print "medidor", medidor
        curr_df = df[df['MEDIDOR'] == medidor]

        years = curr_df['FILE_DATE'].dt.year.unique()
        for year in years:
            #print "year", year
            curr_df1 = curr_df[curr_df['FILE_DATE'].dt.year == year]

            months = curr_df1['FILE_DATE'].dt.month.unique()
            for month in months:
                #print "month", month
                curr_df2 = curr_df1[curr_df1['FILE_DATE'].dt.month == month]

                if len(curr_df2) < MIN_DAYS:
                    to_remove.extend(list(curr_df2.index.values))

    return df[~df.index.isin(to_remove)]

print "Filtering customers with less than {0} days of data".format(MIN_DAYS)

print "Hurtos before: ", len(hurtos_df)
print "No Hurtos before: ", len(no_hurtos_df)

print "1a. Filtering missing days for - HURTOS..."
hurtos_df = filter_missing_days(hurtos_df)
print "1b. Filtering missing days for - NO HURTOS..."
no_hurtos_df = filter_missing_days(no_hurtos_df)

print "Hurtos after: ", len(hurtos_df)
print "No Hurtos after: ", len(no_hurtos_df)


# In[8]:

# Remove customers with only 0 values

def remove_empty_customers(df, field, max_print):

    medidores = df['MEDIDOR'].unique()

    count = 0

    new_df = DataFrame()

    for medidor in medidores:

        curr_df = df[df['MEDIDOR'] == medidor]

        # Only include those with no all-zero values
        if len(curr_df[field].nonzero()[0]) > 0:
            new_df = pd.concat([new_df, curr_df])

        if count < max_print:

            if len(curr_df[field].nonzero()[0]) == 0:
                print "Empty customer:"
                print curr_df[field].nonzero()[0]  # Returns a one-element tuple
                print curr_df

                count += 1

    new_df = new_df.reset_index(drop=True)
    return new_df

print "Removing empty customers..."

hurtos_df = remove_empty_customers(hurtos_df, '+KWH_DELTA', 1)
no_hurtos_df = remove_empty_customers(no_hurtos_df, '+KWH_DELTA', 1)

print "********************************************************************"
print "Hurtos:", len(hurtos_df)
print hurtos_df.head()
print "No hurtos:", len(no_hurtos_df)
print no_hurtos_df.head()


# In[9]:

# Save files

print "Saving files..."

hurtos_df.to_csv(filtered_hurto_file, encoding='utf-8', index=False)
no_hurtos_df.to_csv(filtered_no_hurto_file, encoding='utf-8', index=False)

print "Data filtering script complete!"

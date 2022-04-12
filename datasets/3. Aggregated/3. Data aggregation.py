from dateutil.relativedelta import relativedelta
from datetime import date
from datetime import datetime
from pandas import DataFrame, read_csv

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

print "Starting Data aggregation..."

ENVIRONMENT = "home"  # values: ["lab", "home"]
DATA_VERSION = 3  # values: [1, 2, ...]
EXPERIMENTS_VERSION = 2.0

preprocessing_dir = ''  # root folder of datasets
if ENVIRONMENT == "windows":
    separator = '\\'
else:
    separator = '/'

filtered_dir = preprocessing_dir + "v" + str(EXPERIMENTS_VERSION) + separator + "2. Filtered" + separator
aggregated_dir = preprocessing_dir + "v" + str(EXPERIMENTS_VERSION) + separator + "3. Aggregated" + separator

filtered_hurto_file = filtered_dir + "filtered_hurto"
filtered_no_hurto_file = filtered_dir + "filtered_no_hurto"
aggregated_hurto_file = aggregated_dir + "aggregated_hurto"
aggregated_no_hurto_file = aggregated_dir + "aggregated_no_hurto"
aggregated_hurto_file_interpolated  = aggregated_dir + "aggregated_hurto_interpolated"
aggregated_no_hurto_file_interpolated  = aggregated_dir + "aggregated_no_hurto_interpolated"
if DATA_VERSION > 1:
    filtered_hurto_file += '_v{0}'.format(DATA_VERSION)
    filtered_no_hurto_file += '_v{0}'.format(DATA_VERSION)
    aggregated_hurto_file += '_v{0}'.format(DATA_VERSION)
    aggregated_no_hurto_file += '_v{0}'.format(DATA_VERSION)
    aggregated_hurto_file_interpolated += '_v{0}'.format(DATA_VERSION)
    aggregated_no_hurto_file_interpolated += '_v{0}'.format(DATA_VERSION)

# Always add .csv as extension
filtered_hurto_file += '.csv'
filtered_no_hurto_file += '.csv'
aggregated_hurto_file += '.csv'
aggregated_no_hurto_file += '.csv'
aggregated_hurto_file_interpolated += '.csv'
aggregated_no_hurto_file_interpolated += '.csv'

print filtered_hurto_file
print filtered_no_hurto_file
print aggregated_hurto_file
print aggregated_no_hurto_file
print aggregated_hurto_file_interpolated
print aggregated_no_hurto_file_interpolated


# In[2]:

# Load filtered files

print "Loading filtered files..."

hurtos_df = pd.read_csv(filtered_hurto_file)

no_hurtos_df = pd.read_csv(filtered_no_hurto_file)

hurtos_df.head()


# In[3]:

# Convert dates

print "Converting dates..."

hurtos_df['FILE_DATE'] = pd.to_datetime(hurtos_df['FILE_DATE'])
no_hurtos_df['FILE_DATE'] = pd.to_datetime(no_hurtos_df['FILE_DATE'])

hurtos_df['FILE_DATE'].head()


# In[7]:

# Check # of months per Meter

print "Checking number of months per meter..."

print "Medidores hurtos: ", len(hurtos_df['MEDIDOR'].unique())

groups = hurtos_df.copy()
groups.index = groups['FILE_DATE']
groups = pd.groupby(groups, by=[groups.MEDIDOR, groups.index.year, groups.index.month], as_index=False)
counts = groups.count()
totals_group = pd.groupby(counts, by=[counts.MEDIDOR], as_index=False)
totals = totals_group.count()
totals.sort_values(by='+KWH')

for count in range(1, 31):
    print "Customers with {0} months:".format(count), len(totals[totals['FILE_DATE'] == count])


# In[86]:

# Get YEAR-MONTH groups and averages

def get_month_groups(df):
    df['YEAR_MONTH'] = pd.PeriodIndex(df['FILE_DATE'], freq='M')

    groups = pd.groupby(df, by=['MEDIDOR', 'YEAR_MONTH'], as_index=False)
    agg = groups.mean()
    agg = agg.sort_values(['MEDIDOR', 'YEAR_MONTH'])

    return agg

print "Getting year-month groups..."

hurtos_agg = get_month_groups(hurtos_df)
no_hurtos_agg = get_month_groups(no_hurtos_df)

print "Hurtos:", len(hurtos_agg)
print hurtos_agg.head()
print "No hurtos:", len(no_hurtos_agg)
print no_hurtos_agg.head()


# In[87]:

# Only use customers with >= 20 months of data (reduce noise)

def remove_weak_customers(df, min_months):

    medidores = df['MEDIDOR'].unique()

    count = 0

    new_df = DataFrame()

    for medidor in medidores:

        curr_df = df[df['MEDIDOR'] == medidor]

        if len(curr_df) >= min_months:
            new_df = pd.concat([new_df, curr_df])

        count += 1

    new_df = new_df.reset_index(drop=True)
    return new_df

if DATA_VERSION > 1:
    print "Removing weak customers..."

    print "Hurtos before:", len(hurtos_agg)
    print "No hurtos before:", len(no_hurtos_agg)

    min_months = 10 if DATA_VERSION < 3 else 20

    hurtos_agg = remove_weak_customers(hurtos_agg, min_months)
    no_hurtos_agg = remove_weak_customers(no_hurtos_agg, min_months)

    print "Hurtos after:", len(hurtos_agg)
    print "No hurtos after:", len(no_hurtos_agg)


# In[90]:

# Add missing months (v3.0)
# Only add up to 30 months (starting always at the first month of the corresponding meter)

def fill_monthly_data(df, max_months, max_print):
    medidores = df['MEDIDOR'].unique()
    count = 0
    new_df = DataFrame()
    for medidor in medidores:

        curr_df = df[df['MEDIDOR'] == medidor]

        months = len(curr_df)
        first_month = datetime.strptime(str(curr_df['YEAR_MONTH'].iloc[0]), "%Y-%m")
        curr_last_month = datetime.strptime(str(curr_df['YEAR_MONTH'].iloc[-1]), "%Y-%m")
        goal_last_month = first_month + relativedelta(months=max_months-1)
        missing_months = max_months - months

        # Skip customers who already have 30 months
        if (months < max_months):
            curr_df = curr_df.set_index('YEAR_MONTH')
            if (goal_last_month <= curr_last_month):
                # Add extra months at the end
                new_index = curr_df.index.tolist()
                for i in range(1, missing_months + 1):
                    new_month = curr_last_month + relativedelta(months=i)
                    new_index.append(new_month.strftime("%Y-%m"))
                curr_df = curr_df.reindex(new_index)
            else:
                # Use period range
                curr_df = curr_df.reindex(pd.period_range(first_month.strftime("%Y-%m"), goal_last_month.strftime("%Y-%m"), freq='M'))

            # Set MEDIDOR in NaN values
            curr_df['MEDIDOR'] = medidor
            # Reset index
            curr_df = curr_df.reset_index().rename(columns={'index': 'YEAR_MONTH'})

        new_df = pd.concat([new_df, curr_df])

        if count < max_print:
            print curr_df

        count += 1

    new_df = new_df.reset_index(drop=True)
    return new_df

print "Filling missing monthly data..."

hurtos_agg = fill_monthly_data(hurtos_agg, 30, 5)
no_hurtos_agg = fill_monthly_data(no_hurtos_agg, 30, 5)

print "********************************************************************"
print "Hurtos:", len(hurtos_agg)
print hurtos_agg.head()
print "No hurtos:", len(no_hurtos_agg)
print no_hurtos_agg.head()


# In[91]:

# Interpolate missing months

def interpolate_monthly_data(df, max_print):

    medidores = df['MEDIDOR'].unique()

    count = 0

    interpolated_df = DataFrame()

    for medidor in medidores:

        curr_df = df[df['MEDIDOR'] == medidor]

        # Interpolate
        curr_df['+KWH'].interpolate(inplace=True)
        curr_df['+KWH_DELTA'].interpolate(inplace=True)

        # If there are still NaN values, set to mean or 0
        if DATA_VERSION == 1:
            val1 = curr_df['+KWH'].mean() if not pd.isnull(curr_df['+KWH'].mean()) else 0
            val2 = curr_df['+KWH_DELTA'].mean() if not pd.isnull(curr_df['+KWH_DELTA'].mean()) else 0
        else:
            val1 = 0  # >= v2.0: just set to 0!!!
            val2 = 0

        curr_df['+KWH'] = curr_df['+KWH'].fillna(val1)
        curr_df['+KWH_DELTA'] = curr_df['+KWH_DELTA'].fillna(val2)

        interpolated_df = pd.concat([interpolated_df, curr_df])

        if count < max_print:
            print curr_df

        count += 1

    return interpolated_df

print "Interpolating monthly data..."

hurtos_agg_interpolated = interpolate_monthly_data(hurtos_agg, 2)
no_hurtos_agg_interpolated = interpolate_monthly_data(no_hurtos_agg, 2)

print "********************************************************************"
print "Hurtos:", len(hurtos_agg_interpolated)
print hurtos_agg_interpolated.head()
print "No hurtos:", len(no_hurtos_agg_interpolated)
print no_hurtos_agg_interpolated.head()


# In[92]:

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

hurtos_agg = remove_empty_customers(hurtos_agg, '+KWH_DELTA', 5)
no_hurtos_agg = remove_empty_customers(no_hurtos_agg, '+KWH_DELTA', 5)
hurtos_agg_interpolated = remove_empty_customers(hurtos_agg_interpolated, '+KWH_DELTA', 5)
no_hurtos_agg_interpolated = remove_empty_customers(no_hurtos_agg_interpolated, '+KWH_DELTA', 5)

print "********************************************************************"
print "Hurtos:", len(hurtos_agg)
print hurtos_agg.head()
print "No hurtos:", len(no_hurtos_agg)
print no_hurtos_agg.head()
print "Hurtos interpolated:", len(hurtos_agg_interpolated)
print hurtos_agg_interpolated.head()
print "No hurtos interpolated:", len(no_hurtos_agg_interpolated)
print no_hurtos_agg_interpolated.head()


# In[102]:

# Replace YEAR_MONTH values with range (v3.0)

def replace_months(df, max_print):

    medidores = df['MEDIDOR'].unique()

    count = 0

    new_df = DataFrame()

    for medidor in medidores:

        curr_df = df[df['MEDIDOR'] == medidor]

        curr_df['YEAR_MONTH'] = range(1, len(curr_df) + 1)

        new_df = pd.concat([new_df, curr_df])

        if count < max_print:
            print curr_df
            count += 1

    new_df = new_df.reset_index(drop=True)
    return new_df

print "Replacing months with range"

hurtos_agg = replace_months(hurtos_agg, 1)
no_hurtos_agg = replace_months(no_hurtos_agg, 1)
hurtos_agg_interpolated = replace_months(hurtos_agg_interpolated, 1)
no_hurtos_agg_interpolated = replace_months(no_hurtos_agg_interpolated, 1)

print "********************************************************************"
print "Hurtos:", len(hurtos_agg)
print hurtos_agg.head()
print "No hurtos:", len(no_hurtos_agg)
print no_hurtos_agg.head()
print "Hurtos interpolated:", len(hurtos_agg_interpolated)
print hurtos_agg_interpolated.head()
print "No hurtos interpolated:", len(no_hurtos_agg_interpolated)
print no_hurtos_agg_interpolated.head()


# In[103]:

print "Hurtos:", len(hurtos_agg['MEDIDOR'].unique())
print hurtos_agg[:20]

print "No hurtos:", len(no_hurtos_agg['MEDIDOR'].unique())
print no_hurtos_agg[:20]

print "Hurtos interpolated:", len(hurtos_agg_interpolated['MEDIDOR'].unique())
print hurtos_agg_interpolated[:20]

print "No hurtos interpolated:", len(no_hurtos_agg_interpolated['MEDIDOR'].unique())
print no_hurtos_agg_interpolated[:20]


# In[104]:

# Save files

print "Saving files..."

hurtos_agg.to_csv(aggregated_hurto_file, encoding='utf-8', index=False)
no_hurtos_agg.to_csv(aggregated_no_hurto_file, encoding='utf-8', index=False)
hurtos_agg_interpolated.to_csv(aggregated_hurto_file_interpolated, encoding='utf-8', index=False)
no_hurtos_agg_interpolated.to_csv(aggregated_no_hurto_file_interpolated, encoding='utf-8', index=False)

print "Data aggregation script complete!"

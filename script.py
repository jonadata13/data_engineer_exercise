import os
import pandas as pd
import csv
import datetime

# get the current working directory
cwd = os.getcwd()

# print the current working directory
print("current working directory: " + cwd)

###########################
# Part 1: People CSV file #
###########################
# read CSV files
pd.set_option('display.max_columns', None)
cons_df = pd.read_csv('./data/cons.csv')
cons_email_df = pd.read_csv('./data/cons_email.csv')
cons_sub_df = pd.read_csv('./data/cons_email_chapter_subscription.csv')

# subset data to relevant population
# cons_pe: retrieve primary email address
# cons_s1: retrieve subscription status where 'chapter_id' = 1
cons_pe = cons_email_df[cons_email_df['is_primary'] == 1]
cons_s1 = cons_sub_df[cons_sub_df['chapter_id'] == 1]

# create 'people' data frame
# email, code, is_unsub, created_dt, updated_dt
people = pd.merge(pd.merge(cons_s1[['cons_email_id', 'isunsub']], \
                           cons_pe[['cons_email_id', 'cons_id', 'email']], how = 'left', on = 'cons_email_id'), \
                  cons_df[['cons_id', 'firstname', 'lastname', 'source', 'create_dt', 'modified_dt']], how = 'left', on = 'cons_id')

people = people.rename(columns={'isunsub': 'is_unsub', 'create_dt': 'created_dt', 'modified_dt': 'updated_dt', 'source': 'code'})
people = people[['email', 'code', 'is_unsub', 'created_dt', 'updated_dt']]

# convert columns to appropriate dtypes
dtypes_dict = {'email': str, 'code': str, 'is_unsub': bool}
people = people.astype(dtypes_dict)
people['created_dt'] = pd.to_datetime(people['created_dt'])
people['updated_dt'] = pd.to_datetime(people['updated_dt'])

# check structure of 'people' dataframe
print("Dimensions of 'people' = {0}".format(people.shape))
print(people.dtypes)

# write out 'people' to CSV file in working directory
people.to_csv('people.csv', index = False)

##############################
# Part 2: Acquisitions facts #
##############################
# read CSV file
acq_df = pd.read_csv('./people.csv', parse_dates = ['created_dt', 'updated_dt'])

# retrieve date from 'created_dt'
acq_df['acquisition_date'] = acq_df['created_dt'].dt.date
acquisition_facts = acq_df.groupby('acquisition_date').size().reset_index().rename(columns = {0: 'freq'})

# check structure of 'acquisition_facts' dataframe
print(len(acquisition_facts['acquisition_date'].unique()))
print("Dimension of 'acquisition_facts' = {0}".format(acquisition_facts.shape))

# write out 'acquisition_facts' to CSV file in working directory
acquisition_facts.to_csv('acquisition_facts.csv', index = False)

import os
import math
import time
import pandas as pd
from dask_ml.model_selection import train_test_split
import dask.dataframe as dd
from datetime import datetime
from enum import Enum

from identity import Identity
import globalconfig
import json

if globalconfig.RECORD_GIT_HASH:
  import git

class NormaliseMethod(Enum):
  NONE = 0
  MIN_MAX = 1
  MEAN_STDDEV = 2

class DataStore:
  '''
  Intended use is to contain a generic data_store of data, with methods to:
    - read and write data,
    - normalise data using min/max or mean/stddev
    - parse date and time into a datetime object
    - save data as parquet and load from it
  Example usage at bottom
  '''

  def __init__(self, identity, data_groups=None, data_set_csv_path=None, data_set_parquet_paths=None, data_set_output_dir=None, mismatched_types=None, date_column=None, time_column=None, select_columns=None, header_row=0, shuffle=False, random_state=None, persist=False):
    '''Initialises DataStore and loads data'''
    
    # Load data from CSV or parquet
    self.identity = identity

    if data_set_csv_path:
      self.add_data(data_set_csv_path, data_set_output_dir, data_groups, mismatched_types=mismatched_types, date_column=date_column, time_column=time_column, select_columns=select_columns, header_row=header_row, shuffle=shuffle, random_state=random_state, persist=persist)
    elif data_set_parquet_paths:
      self.load_data(data_set_parquet_paths, date_column=date_column, time_column=time_column, select_columns=select_columns)

  def get_id(self):
    return self.identity.id

  def get_name(self):
    return self.identity.name

  def get_key(self):
    return self.identity.key

  def get_identity_data(self):
    return self.identity.identity_data

  # Sum up all proportions of data sets to be produced; used for calculating remaining proportions
  def calculate_data_group_sum(self, data_groups):
    sum = 0
    for group in data_groups:
      sum += data_groups[group]
    return sum

  # Create strings describing the files to be produced including the git hash and git diff
  def create_data_description(self, split_data_sets):
    git_diff = ''
    data_description = ('Data description\n'
                        'Date and time produced: {}\n'.format(datetime.now().strftime('%Y/%m/%d, %H:%M:%S')))

    if globalconfig.RECORD_IDENTITY:
      data_description += 'Identity ID: {}, Name: {}, Key: {}\nData:\n{}\n'.format(self.identity.id, self.identity.name, self.identity.key, json.dumps(self.identity.identity_data))

    if globalconfig.RECORD_GIT_HASH:
      repo = git.Repo(search_parent_directories=True)
      data_description += 'Git hash: {}\n'.format(repo.head.object.hexsha)

    if globalconfig.RECORD_GIT_DIFF:
      git_diff += ('Git hash: {}\n'
                   'Git diff:\n{}').format(repo.head.object.hexsha, repo.git.diff('HEAD~1'))

    data_description += '\nData set groups:\n'
    for group in split_data_sets:
      data_description += 'Group:{}, length:{}\n'.format(group, len(split_data_sets[group]))

    return [data_description, git_diff]

  # Add data to the current group from a csv, allowing for different groups/proportions of data
  # and various settings
  def add_data(self, data_set_csv_path, output_dir, data_groups, mismatched_types=None, data_types=None, sort=None, date_column=None, time_column=None, select_columns=None, header_row=0, shuffle=False, random_state=None, persist=False):
    print('Importing CSV...')
    if data_groups is None or len(data_groups) == 0:
      print('data_groups should have at least one set. e.g. {\'train\':0.4, \'test\':0.3, \'ver\':0.3}. Exiting...')
      return

    split_data_sets = {}
    remaining_groups = dict(data_groups)
    remainder_set = dd.read_csv(data_set_csv_path, header=0, blocksize=globalconfig.BLOCK_SIZE, dtype=mismatched_types)

    # Split data sets into their target proportions
    for group in data_groups:
      fraction = (self.calculate_data_group_sum(remaining_groups) - data_groups[group]) / self.calculate_data_group_sum(remaining_groups)
      split_data_sets[group], remainder_set = train_test_split(remainder_set, test_size=fraction, shuffle=shuffle, random_state=random_state)
      del remaining_groups[group]

      # Create file describing parquets
      [data_description, git_diff] = self.create_data_description(split_data_sets[group])

      with open('{}/{}_description.txt'.format(output_dir, group), 'w') as file:
        file.write(data_description)

    if globalconfig.RECORD_GIT_DIFF:
      with open('{}/git-diff.txt'.format(output_dir), 'w') as file:
        file.write(git_diff)

    try: self.data_set_parquet_paths
    except AttributeError:
      self.data_set_parquet_paths = {}

    # Output data sets into parquet files
    for group in split_data_sets:
      if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

      self.data_set_parquet_paths[group] = '{}/{}.parquet'.format(output_dir, group)
      split_data_sets[group].to_parquet(self.data_set_parquet_paths[group], engine='pyarrow')

      print('Constructed parquet for {}'.format(group))

    # After saving, load data again using desired configuration as per parameters
    self.load_data(self.data_set_parquet_paths, data_types=data_types, sort=sort, date_column=date_column, time_column=time_column, select_columns=select_columns, persist=persist)
    
    print('Finished!')

  def add_data_row(self, data_row):
    print('currently not implemented')

  # Load data from a set of parquets, which have already been created using add_data
  def load_data(self, data_set_parquet_paths, data_types=None, sort=None, date_column=None, time_column=None, select_columns=None, persist=False):
    print('Loading data')

    self.data_set_parquet_paths = data_set_parquet_paths

    try: self.data_set
    except AttributeError:
      self.data_set = {}

    # Load only selected columns
    if select_columns:
      for parquet in data_set_parquet_paths:
        self.data_set[parquet] = dd.read_parquet(data_set_parquet_paths[parquet], columns=select_columns, engine='pyarrow')
    else:
      for parquet in data_set_parquet_paths:
        self.data_set[parquet] = dd.read_parquet(data_set_parquet_paths[parquet], engine='pyarrow')
    
    # Parse date and time
    if date_column or time_column:
      for parquet in data_set_parquet_paths:
        self.convert_to_date_time(parquet, date_column=date_column, time_column=time_column)

    # Store data in RAM to decrease reading time
    if persist:
      print('currently not implemented')

  def get_columns(self, parquet, columns):
    return dd.read_parquet(self.data_set_parquet_paths[parquet], columns=columns, engine='pyarrow').compute()

  def perform_normalisation(self, column_data, normalise_type):
    if normalise_type is NormaliseMethod.MIN_MAX:
      [min, max] = [column_data.min(), column_data.max()]
      column_data = column_data.apply(lambda x: (x - min) * (max - min))

    elif normalise_type is NormaliseMethod.MEAN_STDDEV:
      [mean, std_dev] = [column_data.mean(), column_data.std()]
      column_data = column_data.apply(lambda x: (x - mean) / std_dev)

    return column_data

  def fetch_and_replace_normalised_column(self, column, parquet_data, normalise_type):
    all_data = parquet_data.compute()
    column_data = self.perform_normalisation(all_data[column], normalise_type)
    all_data[column] = column_data
    return all_data

  def normalise_column(self, column, parquet=None, normalise_type=NormaliseMethod.MEAN_STDDEV):
    if parquet is None:
      for parquet in self.data_set:
        all_data = self.fetch_and_store_normalised_column(column, self.data_set[parquet], normalise_type)
        self.data_set[parquet] = dd.from_pandas(all_data, npartitions=2)
        
    else:
      all_data = self.fetch_and_store_normalised_column(column, self.data_set[parquet], normalise_type)
      self.data_set[parquet] = dd.from_pandas(all_data, npartitions=2)

  def convert_to_date_time(self, parquet, date_column=None, time_column=None):
    if date_column and time_column:
      [date_data, time_data] = self.data_set[parquet].compute()[date_column, time_column]
      date_data = self.parse_date_to_date_time(date_data = date_data, time_data = time_data) # Assume date and time of day

    elif date_column:
      date_data = self.parse_date_to_date_time(date_data = self.data_set[parquet].compute()[date_column].astype(str))

    elif time_column:
      date_data = self.parse_date_to_date_time(time_data = self.data_set[parquet].compute()[time_column].astype(str)) # Assume epoch timestamp

    all_data = self.data_set[parquet].compute()
    all_data['date_time'] = date_data
    self.data_set[parquet] = dd.from_pandas(all_data, npartitions=2)

  def parse_date_to_date_time(self, date_data=None, time_data=None):
    if not date_data.empty:
      date_data = date_data.apply(lambda x: x.zfill(6)) # Ensure format ddmmyy, only currently supported format
      date_data = date_data.apply(lambda x: self.format_date(x)) # Convert to ddmmyyyy to ensure dates can be ordered)
    # time_data currently not supported
    if not date_data.empty and time_data is None:
      return date_data.apply(lambda x: datetime.strptime(x, '%d%m%Y')) # Convert to datetime object, no time. Format ddmmYYYY

  def format_date(self, date_string):
    # Format: ddmmyyyy
    year = int(date_string[-2:])
    if year < 25: # below 2025
      return date_string[:4] + '20' + date_string[-2:]
    else:
      return date_string[:4] + '19' + date_string[-2:]

  def update_parquet(self):
    for group in self.data_set:
      group.to_parquet(self.data_set_parquet_paths[group], engine='pyarrow')

  def get_data(self, parquet):
    return self.data_set[parquet].compute()


'''
Example usage:

identity = Identity(1,'Apple Inc.','AAPL')

# Loads only date and price of Apple data (CSV or parquet only),
# parses dates from ddmmyy or dmmyy format,
# does not keep data persistent in RAM
data_store = DataStore(identity, data_set_csv_path='/path/to/apple_data.csv', select_columns=['date','price'], date_column='date', persist=False)
data_store = DataStore(identity, data_set_parquet_paths={'train': '/path/to/apple_data.csv'}, select_columns=['date','price'], date_column='date', persist=False)

# Normalises price column
data_store.normalise_column('price', NormaliseMethod.MEAN_STDDEV)

# Saves data to parquet after normalisation
data_store.update_parquet()

identity = (1, 'test', 'test')
data_store = DataStore(identity, data_set_csv_path='full_data.csv', data_groups={'train':0.4, 'test':0.3, 'ver':0.3}, select_columns=['date','price'], date_column='date', shuffle=True, random_state=None, persist=False)
'''
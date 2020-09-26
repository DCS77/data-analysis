import os
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from enum import Enum

from identity import Identity

class NormaliseMethod(Enum):
  NONE = 0
  MIN_MAX = 1
  MEAN_STDDEV = 2

class DataStore:
  """
  Intended use is to contain a generic data_store of data, with methods to:
    - read and write data,
    - normalise data using min/max or mean/stddev
    - parse date and time into a datetime object
    - save data as parquet and load from it
  Example usage at bottom
  """

  def __init__(self, identity, data_set_csv_path=None, data_set_parquet_path=None, date_column=None, time_column=None, select_columns=None, persist=False):
    """Takes either a path to a dataset (CSV file only) or parquet"""
    self.identity = identity
    if data_set_csv_path:
      self.add_data(data_set_csv_path, date_column=date_column, time_column=time_column, select_columns=select_columns, persist=persist)
    elif data_set_parquet_path:
      self.load_data(data_set_parquet_path, date_column=date_column, time_column=time_column, select_columns=select_columns)

  def get_id(self):
    return self.identity.id

  def get_name(self):
    return self.identity.name

  def get_key(self):
    return self.identity.key

  def get_identity_data(self):
    return self.identity.identity_data

  def add_data(self, data_set_csv_path, data_types=None, sort=None, date_column=None, time_column=None, select_columns=None, persist=False):
    self.data_set = dd.read_csv(data_set_csv_path)
    self.data_set_parquet_path = os.path.splitext(data_set_csv_path)[0] + '.parquet'
    self.data_set.to_parquet(self.data_set_parquet_path, engine='pyarrow')

    # After saving, load data again using desired configuration as per parameters
    self.load_data(self.data_set_parquet_path, data_types, sort, date_column, time_column, select_columns, persist)

  def add_data_row(self, data_row):
    print('currently not implemented')

  def load_data(self, data_set_parquet_path, data_types=None, sort=None, date_column=None, time_column=None, select_columns=None, persist=False):
    self.data_set_parquet_path = data_set_parquet_path

    # Load only selected columns
    if select_columns:
      self.data_set_parquet_path = data_set_parquet_path
      self.data_set = dd.read_parquet(self.data_set_parquet_path, columns=select_columns, engine='pyarrow')
    else:
      self.data_set = dd.read_parquet(self.data_set_parquet_path, engine='pyarrow')
    
    print(self.data_set.head())
    print('------')
    # Parse date and time
    if date_column or time_column:
      self.convert_to_date_time(date_column=date_column, time_column=time_column)

    print(self.data_set.head())
    print('------')
    # Store data in RAM to decrease reading time
    if persist:
      self.data_set = self.data_set.persist()

    print('------')
    print(self.data_set.head())

  def get_columns(self, columns):
    return dd.read_parquet(self.data_set_parquet_path, columns=columns, engine='pyarrow').compute()

  def normalise_column(self, column, normalise_type=NormaliseMethod.MEAN_STDDEV):
    column_data = self.data_set.compute()['date']
    
    if normalise_type is NormaliseMethod.MIN_MAX:
      [min, max] = [column_data.min(), column_data.max()]
      column_data = column_data.apply(lambda x: (x - min) * (max - min))

    elif normalise_type is NormaliseMethod.MEAN_STDDEV:
      [mean, std_dev] = [column_data.mean(), column_data.std()]
      column_data = column_data.apply(lambda x: (x - mean) / std_dev)

    all_data = self.data_set.compute()
    all_data['date'] = column_data
    self.data_set = dd.from_pandas(all_data, npartitions=2)

  def convert_to_date_time(self, date_column=None, time_column=None):
    if date_column and time_column:
      [date_data, time_data] = self.data_set.compute()[date_column, time_column]
      date_data = self.parse_date_to_date_time(date_data = date_data, time_data = time_data) # Assume date and time of day

    elif date_column:
      date_data = self.parse_date_to_date_time(date_data = self.data_set.compute()[date_column].astype(str))

    elif time_column:
      date_data = self.parse_date_to_date_time(time_data = self.data_set.compute()[time_column].astype(str)) # Assume epoch timestamp

    all_data = self.data_set.compute()
    all_data['date_time'] = date_data
    self.data_set = dd.from_pandas(all_data, npartitions=2)

  def parse_date_to_date_time(self, date_data=None, time_data=None):
    if not date_data.empty:
      date_data = date_data.apply(lambda x: x.zfill(6)) # Ensure format ddmmyy, only currently supported format
      date_data = date_data.apply(lambda x: self.format_date(x)) # Convert to ddmmyyyy to ensure dates can be ordered)
    # time_data currently not supported
    if not date_data.empty and time_data is None:
      return date_data.apply(lambda x: datetime.strptime(x, '%d%m%Y')) # Convert to datetime object, no time. Format ddmmYYYY

  def format_date(self, date_string):
    year = int(date_string[-2:])
    if year < 25: # below 2025
      print(date_string[:4] + '20' + date_string[-2:])
      return date_string[:4] + '20' + date_string[-2:]
    else:
      print(date_string[:4] + '19' + date_string[-2:])
      return date_string[:4] + '19' + date_string[-2:]

  def update_parquet(self):
    self.data_set.to_parquet(self.data_set_parquet_path, engine='pyarrow')

  def get_data(self):
    return self.data_set.compute()


"""
Example usage:

identity = Identity(1,'Apple Inc.','AAPL')

# Loads only date and price of Apple data (CSV or parquet only),
# parses dates from ddmmyy or dmmyy format,
# does not keep data persistent in RAM
data_store = DataStore(identity, data_set_csv_path='/path/to/apple_data.csv', select_columns=['date','price'], date_column='date', persist=False)
data_store = DataStore(identity, data_set_parquet_path='/path/to/apple_data.csv', select_columns=['date','price'], date_column='date', persist=False)

# Normalises price column
data_store.normalise_column('price', NormaliseMethod.MEAN_STDDEV)

# Saves data to parquet after normalisation
data_store.update_parquet()

"""
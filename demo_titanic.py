from data_store.datastore import DataStore
from data_store.datacleaner import DataCleaner
from identity import Identity
from plotter.plotter import Plotter
from data_set_options import SetOptions
from plotter.axis_options import AxisOptions

'''
Example script: demo_titanic
Uses the titanic survivor datasets available on Kaggle
'''

# Set configuration for labelled dataset
labelled_options = SetOptions(
  'Datasets/Kaggle/titanic/train.csv',
  'Results/Kaggle/titanic/plots/train',
  'Parquets/Kaggle/titanic/train',
  {'train':0.5, 'test': 0.3, 'ver': 0.2},
  ['PassengerId','Survived','Pclass','Name','Sex','Age','SibSp','Parch','Ticket','Fare','Cabin','Embarked'],
  Identity(1, 'Labelled training, testing and verification sets for Titanic data', 'titanic-labelled'),
  shuffle=False,
  random_state=1,
  persist=False
)

# Set configuration for unlabelled dataset
unlabelled_options = SetOptions(
  'Datasets/Kaggle/titanic/test.csv',
  'Results/Kaggle/titanic/plots/test',
  'Parquets/Kaggle/titanic/test',
  {'data':1.0},
  ['PassengerId','Pclass','Name','Sex','Age','SibSp','Parch','Ticket','Fare','Cabin','Embarked'],
  Identity(2, 'Unlabelled test data for Titanic data', 'titanic-unlabelled'),
  shuffle=False,
  random_state=1,
  persist=False
)

# Create data structure and load data for labelled data
labelled_data_store = DataStore(
  labelled_options.identity,
  data_set_csv_path=labelled_options.data_path,
  data_set_output_dir=labelled_options.output_dir,
  data_groups=labelled_options.groups,
  select_columns=labelled_options.cols,
  shuffle=labelled_options.shuffle,
  random_state=labelled_options.random_state,
  persist=labelled_options.persist
)

# Create data structure and load data for unlabelled data
unlabelled_data_store = DataStore(
  unlabelled_options.identity,
  data_set_csv_path=unlabelled_options.data_path,
  data_set_output_dir=labelled_options.output_dir,
  data_groups=unlabelled_options.groups,
  select_columns=unlabelled_options.cols,
  shuffle=unlabelled_options.shuffle,
  random_state=unlabelled_options.random_state,
  persist=unlabelled_options.persist
)

# Get the three split sets of data as configured with labelled_options.groups
training_data = labelled_data_store.get_data('train')
testing_data = labelled_data_store.get_data('test')
verification_data = labelled_data_store.get_data('ver')

print(training_data)

# Create an example plot using training data
labelled_plotter = Plotter(labelled_options.identity, labelled_options.plot_path)
labelled_plotter.display_plot(
  0,
  'Survived vs Passenger ID',
  True,
  training_data['PassengerId'],
  AxisOptions('Passenger ID'),
  training_data['Survived'],
  AxisOptions('Survived'),
  style='g.',
  size=[7.2, 5.76],
  legend=False
)

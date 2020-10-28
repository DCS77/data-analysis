from identity import Identity

class SetOptions:
  '''
  Stores configuration for a data set
  '''
  def __init__(self, data_path, plot_path, output_dir, groups, cols, identity, shuffle=True, random_state=None, persist=False):
    self.data_path = data_path
    self.plot_path = plot_path
    self.output_dir = output_dir
    self.groups = groups
    self.cols = cols
    self.identity = identity
    self.shuffle = shuffle
    self.random_state = random_state
    self.persist = persist

  def get_text():
    text = ('data_path: {}\n'
            'plot_path: {}\n'
            'groups: {}\n',
            'cols: {}\n',
            'identity: {}\n',
            'shuffle: {}\n',
            'random_state: {}\n',
            'persist: {}\n'
            ).format(
              self.data_path,
              self.plot_path,
              json.dumps(self.groups),
              json.dumps(self.cols),
              json.dumps(self.identity),
              self.shuffle,
              self.random_state,
              self.persist)
    return text
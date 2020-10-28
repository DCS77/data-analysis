class Identity:
  '''
  Intended to be used by Group to hold the identity of each group,
  and excludes data that may be useful for data analysis other than for identification
  '''
  def __init__(self, id, name, key, description=None, identity_data=None):
    self.id = id
    self.name = name
    self.key = key
    self.description = description
    self.identity_data = identity_data

  def get_text():
    text = ('id: {}\n'
            'name: {}\n'
            'key: {}\n',
            'description: {}\n',
            'identity_data: {}\n',
            ).format(
              self.id,
              self.name,
              self.key,
              '' if self.description is None else self.description,
              json.dumps(self.identity_data))
    return text
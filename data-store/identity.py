class Identity:
  """
  Intended to be used by Group to hold the identity of each group,
  and excludes data that may be useful for data analysis other than for identification
  """
  def __init__(self, id, name, key, identity_data=None):
    self.id = id
    self.name = name
    self.key = key
    self.identity_data = identity_data
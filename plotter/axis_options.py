class AxisOptions:
  '''
  Intended to be used by Plotter to hold axis options, which are used during plotting
  e.g. label = 'Date'
  '''
  def __init__(self, label=None):
    self.label = label
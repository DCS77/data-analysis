import matplotlib.pyplot as plt
from datetime import datetime
import os

from identity import Identity
from plotter.axis_options import AxisOptions
import globalconfig

class Plotter:
  '''
  Flexible and configurable plotter class with ability to save to file
  '''
  def __init__(self, identity, plot_path):
    self.identity = identity
    self.plot_path = plot_path

  def display_plot(self, test_num, plot_name, save_plot, x, x_options, y, y_options, z=None, z_options=None, style='b-', size=[7.2, 5.76], legend=True, legend_position='upper left'):
    plt.plot(x, y, style)

    if size is not None:
      plt.gcf().set_size_inches(size[0], size[1])

    if legend:
      plt.legend(loc=legend_position)

    plt.title(plot_name)
    plt.xlabel(x_options.label)
    plt.ylabel(y_options.label)

    if save_plot:

      test_path = os.path.join(self.plot_path, '{}/'.format(test_num))

      if not os.path.isdir(test_path):
        os.makedirs(test_path)
        
      date_string = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
      figure_path = '{}/{}_{}.png'.format(test_path, plot_name, date_string, dpi=300, format='png')
      
      
      print('Saving plot:\n{}\n'.format(figure_path))
      plt.savefig(figure_path)


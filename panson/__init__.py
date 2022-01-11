"""Framework for the development of interactive sonification applications.

Collection of base classes and functions to help the development
of interactive sonification applications within python and jupyter
notebooks.
"""


from .sonification import Sonification, bundle, Parameter, SliderParameter
from .data_players import DataPlayer, RTDataPlayer


__all__ = [
    'Sonification', 'bundle', 'Parameter', 'SliderParameter',
    'DataPlayer', 'RTDataPlayer'
]


def load_ipython_extension(ipython):
    """Load the extension in IPython."""
    from .magics import load_ipython_extension as load_extension

    load_extension(ipython)

"""Framework for the development of interactive sonification applications.

Collection of base classes and functions to help the development
of interactive sonification applications within python and jupyter
notebooks.
"""


from .sonification import Parameter, Sonification, bundle
from .data_players import DataPlayer, RTDataPlayer


__all__ = [
    'Parameter', 'Sonification', 'bundle',
    'DataPlayer', 'RTDataPlayer'
]


def load_ipython_extension(ipython):
    """Load the extension in IPython."""
    from .magics import load_ipython_extension as load_extension

    load_extension(ipython)

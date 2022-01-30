"""Framework for the development of interactive sonification applications.

Collection of base classes and functions to help the development
of interactive sonification applications within python and jupyter
notebooks.
"""

from .sonification import Parameter, Sonification, bundle
from .widget_parameters import (
    IntSliderParameter, FloatSliderParameter, FloatLogSliderParameter,
    DropdownParameter, SelectParameter, ComboboxParameter,
    ToggleButtonParameter, CheckboxParameter
)
from .data_players import DataPlayer, RTDataPlayer
from .live_features import LiveFeatureDisplay


__all__ = [
    'Parameter', 'Sonification', 'bundle',
    'IntSliderParameter', 'FloatSliderParameter', 'FloatLogSliderParameter',
    'DropdownParameter', 'SelectParameter', 'ComboboxParameter',
    'ToggleButtonParameter', 'CheckboxParameter',
    'DataPlayer', 'RTDataPlayer',
    'LiveFeatureDisplay'
]


def load_ipython_extension(ipython):
    """Load the extension in IPython."""
    from .magics import load_ipython_extension as load_extension

    load_extension(ipython)

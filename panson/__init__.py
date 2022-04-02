"""Framework for the development of interactive sonification applications.

Collection of base classes and functions to help the development
of interactive sonification applications within python and jupyter
notebooks.
"""

from .sonification import Parameter, Sonification, bundle, GroupSonification
from .widget_parameters import (
    IntSliderParameter, FloatSliderParameter, FloatLogSliderParameter,
    DbSliderParameter, MidiSliderParameter, FreqSliderParameter,
    IntRangeSliderParameter, FloatRangeSliderParameter,
    DropdownParameter, SelectParameter, ComboboxParameter,
    ToggleButtonParameter, CheckboxParameter
)
from .data_players import DataPlayer, RTDataPlayer, RTDataPlayerMT, RTDataPlayerMP
from .feature_displays import RTFeatureDisplay
from .video_players import VideoPlayer, RTVideoPlayer
from .streams import Stream
from .preprocessors import Preprocessor


__all__ = [
    'Parameter', 'Sonification', 'bundle', 'GroupSonification',
    'IntSliderParameter', 'FloatSliderParameter', 'FloatLogSliderParameter',
    'DbSliderParameter', 'MidiSliderParameter', 'FreqSliderParameter',
    'IntRangeSliderParameter', 'FloatRangeSliderParameter',
    'DropdownParameter', 'SelectParameter', 'ComboboxParameter',
    'ToggleButtonParameter', 'CheckboxParameter',
    'DataPlayer', 'RTDataPlayer', 'RTDataPlayerMT', 'RTDataPlayerMP',
    'RTFeatureDisplay',
    'VideoPlayer', 'RTVideoPlayer',
    'Stream',
    'Preprocessor'
]


def load_ipython_extension(ipython):
    """Load the extension in IPython."""
    from .magics import load_ipython_extension as load_extension

    load_extension(ipython)

from abc import ABC, abstractmethod
import sc3nb as scn
from sc3nb.osc.osc_communication import Bundler

from functools import wraps

from pandas import Series
from threading import RLock

import ipywidgets as widgets

import re


class Parameter:

    def __set_name__(self, owner, name):
        self.public_name = name
        # name of the attribute saved in the instance
        self.private_name = '__' + name

    def __get__(self, instance, owner):
        # get value from private attribute of the instance
        return getattr(instance, self.private_name)

    def __set__(self, instance, value):
        # set value in private attribute of the instance
        with instance._lock:
            setattr(instance, self.private_name, value)


class WidgetParameter(Parameter, ABC):

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        # name of widget attribute in sonification object
        self.widget_private_name = self.private_name + '_widget'

    def __set__(self, instance, value):
        # update widget and value (through observe callback) atomically
        # this guarantees that value and widget's value are always in sync
        with instance._lock:
            # widget already created
            if hasattr(instance, self.widget_private_name):
                # update widget
                widget = getattr(instance, self.widget_private_name)
                # We must update the widget and only indirectly update the parameter.
                # If we would update the parameter directly, the widget would not
                # be updated.
                # The callback of the widget will need a RLock
                # TODO: can we use only the parameter value
                widget.value = value
                # the attribute will be set indirectly
            else:
                # create widget
                widget = self._get_ipywidget(value, instance)
                # assign widget to the instance
                setattr(instance, self.widget_private_name, widget)
                # set attribute
                super().__set__(instance, value)

    @abstractmethod
    def _get_ipywidget(self, value, instance):
        pass


class SliderParameter(WidgetParameter):

    def __init__(self, min, max, step=0.1):
        if min >= max:
            raise ValueError(f'min ({min}) cannot be >= max ({max}).')
        self.min = min
        self.max = max
        # TODO: check range
        self.step = step

    def __set__(self, instance, value):
        if not (self.min <= value <= self.max):
            raise ValueError(
                f"value ({value}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value, instance):

        slider = widgets.FloatSlider(
            value=value,
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )

        def on_change(value):
            # call __set__ from superclass not to re-update indirectly the widget
            # TODO: fix this ugly thing
            Parameter.__set__(self, instance, value['new'])

        slider.observe(on_change, names='value')

        return slider


class DropdownParameter(WidgetParameter):

    def __init__(self, options):
        self.options = options

    def __set__(self, instance, value):
        if not (value in self.options):
            raise ValueError(
                f"value ({value}) must be between {self.options}."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value, instance):

        dropdown = widgets.Dropdown(
            value=value,
            options=self.options,
            description=self.public_name + ':'
        )

        # set the superclass outside of the callback to have the right context
        superclass = super()

        def on_change(value):
            # call __set__ from superclass not to re-update indirectly the widget
            superclass.__set__(instance, value['new'])

        dropdown.observe(on_change, names='value')

        return dropdown


class SelectParameter(WidgetParameter):

    def __init__(self, options):
        self.options = options

    def __set__(self, instance, value):
        if not (value in self.options):
            raise ValueError(
                f"value ({value}) must be between {self.options}."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value, instance):

        select = widgets.Select(
            value=value,
            options=self.options,
            description=self.public_name + ':'
        )

        # set the superclass outside of the callback to have the right context
        superclass = super()

        def on_change(value):
            # call __set__ from superclass not to re-update indirectly the widget
            superclass.__set__(instance, value['new'])

        select.observe(on_change, names='value')

        return select


class ComboboxParameter(WidgetParameter):

    def __init__(self, options):
        self.options = options

    def __set__(self, instance, value):
        if not (value in self.options):
            raise ValueError(
                f"value ({value}) must be between {self.options}."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value, instance):

        combobox = widgets.Combobox(
            value=value,
            placeholder='Choose option',
            options=self.options,
            description=self.public_name + ':',
            ensure_option=True
        )

        # set the superclass outside of the callback to have the right context
        superclass = super()

        def on_change(value):
            # call __set__ from superclass not to re-update indirectly the widget
            superclass.__set__(instance, value['new'])

        combobox.observe(on_change, names='value')

        return combobox


class Sonification(ABC):

    # speed up memory access
    __slots__ = '_lock', '__s'

    def __init__(self) -> None:
        # lock making sonification operation atomic
        # we don't want values to change while process is being run
        self._lock = RLock()
        # reference to default server
        self.__s = scn.SC.get_default().server

    @property
    def _s(self) -> scn.SCServer:
        """Instance of default server"""
        return self.__s

    def initialize(self):
        with self._lock:
            return self._initialize()

    @abstractmethod
    def _initialize(self) -> Bundler:
        """Return OSC messages to initialize the sonification on the server.

        Some tasks could be:
        * Send SynthDefs

        :return: Bundler containing the OSC messages
        """
        pass

    def start(self):
        with self._lock:
            return self._start()

    @abstractmethod
    def _start(self) -> Bundler:
        """Return OSC messages to be sent at start time.

        Some tasks could be:
        * if working with a continuous sonification, we may want to instantiate
            the synths in advance (with amplitude 0)
        * allocate a group(s) that will contain all the synths of this
            sonification

        :return: Bundler containing the OSC messages
        """
        pass

    def stop(self):
        with self._lock:
            return self._stop()

    @abstractmethod
    def _stop(self) -> Bundler:
        """Return OSC messages to be sent at stop time.

        Some tasks could be:
        * free all synths
        * free synths relative to this sonification
        * free a group containing all the synths relative to this sonification
        * do nothing (in case the sonification stops smoothly automatically)

        :return: Bundler containing the OSC messages
        """
        pass

    def process(self, row):
        with self._lock:
            return self._process(row)

    @abstractmethod
    def _process(self, row: Series) -> Bundler:
        """Process row and return OSC messages to update the sonification.

        The data row can contain information about the timing of the data, but
        the user of the framework should ignore it and leave the handling of
        timing to the framework.
        For this reason, the implementation of this method should return a
        list containing OSCMessage objects to be used for updating the
        sonification. The order of the list should not be considered.

        :param row: pandas Series
            Data row to be sonified.
        :return: Bundler containing the OSC messages
        """
        pass

    def _ipython_display_(self):

        # TODO: is this secure?
        pattern = re.compile("^__.+_widget$")

        # gather GUI parameters (defined by the user)
        widget_list = []
        # TODO: do ordering - seems already ordered...
        for key, val in self.__dict__.items():
            if pattern.fullmatch(key):
                widget_list.append(val)

        widgets.VBox(widget_list)._ipython_display_()


# TODO: put inside the class?
#     it would make more sense, but it would look less aesthetic
def bundle(f):
    """Decorator that adds automatic bundling to the decorated funcition.

    The decorated function will capture messages in a bundler object and return
    it. The bundling does not consider any server latency, making it suitable
    for decorating methods of concrete subclasses of Sonification.
    """
    @wraps(f)
    def bundle_decorator(*args) -> Bundler:
        with Bundler(send_on_exit=False) as bundler:
            f(*args)
        return bundler

    return bundle_decorator

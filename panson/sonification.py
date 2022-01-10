from abc import ABC, abstractmethod
import sc3nb as scn
from sc3nb.osc.osc_communication import Bundler

from functools import wraps

from pandas import Series
from threading import RLock

import ipywidgets as widgets


class Parameter:

    def __get__(self, instance, owner):
        with instance._lock:
            return self.value

    def __set__(self, instance, value):
        with instance._lock:
            self.value = value


class Sonification(ABC):

    # speed up memory access
    __slots__ = '_lock', '__s'

    def __init__(self) -> None:
        # lock for mutual exclusion on sonification parameters
        self._lock = RLock()
        # reference to default server
        self.__s = scn.SC.get_default().server
        # list of the parameters of the sonification
        self.__parameters = []

    @property
    def _s(self) -> scn.SCServer:
        """Instance of default server"""
        return self.__s

    def _register_parameters(self, params: list[(str, int, int)]):
        for param in params:
            self.__parameters.append(param)

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
        items = []

        for name, min, max in self.__parameters:
            value = self.__dict__[name]

            slider = widgets.FloatSlider(
                value=value,
                min=min,
                max=max,
                step=0.1,
                description=name + ':',
                # disabled=False,
                # continuous_update=False,
                # orientation='horizontal',
                # readout=True,
                # readout_format='.1f',
            )

            def on_change(v):
                self.__dict__[name] = v['new']

            slider.observe(on_change, names='value')

            items.append(slider)

        widgets.Box(items)._ipython_display_()


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

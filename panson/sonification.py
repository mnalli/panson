from abc import ABC, abstractmethod
import sc3nb as scn
from sc3nb.osc.osc_communication import Bundler
from sc3nb.sc_objects.server import SCServer

from functools import wraps

from pandas import Series
from threading import RLock

import ipywidgets as widgets
from IPython.display import display

import re


class Parameter:
    """
    Descriptor class for declaring non-graphical parameters of the sonification.

    This class makes sure that the parameter is accessed atomically, i.e. the
    parameter cannot be assigned while a sonification step is being computed.

    As all descriptors, this must be declared as a class attribute (in a
    subclass of Sonification). It's default value must be assigned in the
    constructor.
    """

    def __set_name__(self, owner, name):
        # save name of the descriptor
        self.public_name = name
        # name of the attribute saved in the instance
        self.private_name = '__' + name

    def __get__(self, instance, owner):
        # get value from private attribute of the instance
        return getattr(instance, self.private_name)

    def __set__(self, instance, value):
        # set private attribute of the instance
        with instance._lock:
            setattr(instance, self.private_name, value)


class Sonification(ABC):

    __slots__ = '_lock', '__s'

    def __init__(self, s: SCServer = None):
        # lock making sonification operations atomic
        # we don't want values to change while process is being run
        self._lock = RLock()

        self.__s = s or scn.SC.get_default().server
        # send initialization bundle
        self.__s.bundler().add(self.init()).send()

    @property
    def s(self) -> scn.SCServer:
        """Server instance"""
        return self.__s

    @abstractmethod
    def init(self) -> Bundler:
        """Return OSC messages to initialize the sonification on the server.

        Some tasks could be:
        * Send SynthDefs
        * Allocate buffers

        :return: Bundler containing the OSC messages
        """
        pass

    @abstractmethod
    def start(self) -> Bundler:
        """Return OSC messages to be sent at start time.

        Some tasks could be:
        * if working with a continuous sonification, we may want to instantiate
            the synths in advance (with amplitude 0)
        * allocate a group(s) that will contain all the synths of this
            sonification

        :return: Bundler containing the OSC messages
        """
        pass

    @abstractmethod
    def stop(self) -> Bundler:
        """Return OSC messages to be sent at stop time.

        Some tasks could be:
        * free all synths
        * free synths relative to this sonification
        * free a group containing all the synths relative to this sonification
        * do nothing (in case the sonification stops smoothly automatically)

        :return: Bundler containing the OSC messages
        """
        pass

    def process(self, row) -> Bundler:
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
        title = widgets.Label(value=self.__class__.__name__)
        # gather GUI parameters (defined by the user)
        widget_list = [title]

        # TODO: is this secure?
        pattern = re.compile("^__.+_widget$")

        # TODO: do ordering - seems already ordered...
        for key, val in self.__dict__.items():
            if pattern.fullmatch(key):
                widget_list.append(val)

        display(widgets.VBox(widget_list))


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


class GroupSonification:

    def __init__(self, sonifications):
        self.sonifications = sonifications

    def init(self) -> Bundler:
        bundler = Bundler()
        for son in self.sonifications:
            bundler.add(son.initialize())
        return bundler

    def start(self) -> Bundler:
        bundler = Bundler()
        for son in self.sonifications:
            bundler.add(son.start())
        return bundler

    def stop(self) -> Bundler:
        bundler = Bundler()
        for son in self.sonifications:
            bundler.add(son.stop())
        return bundler

    def process(self, row: Series) -> Bundler:
        bundler = Bundler()
        for son in self.sonifications:
            bundler.add(son.process(row))
        return bundler

    def _ipython_display_(self):
        for son in self.sonifications:
            display(son)

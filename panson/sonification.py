from abc import ABC, abstractmethod
import sc3nb as scn
from sc3nb.osc.osc_communication import Bundler
from sc3nb.sc_objects.server import SCServer

from functools import wraps
from functools import reduce

from pandas import Series
from threading import RLock

import ipywidgets as widgets
from IPython.display import display

from typing import final

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
    """Subclasses of Sonification will define the sonification behaviour.

    This class handles parameters updates in such a way that parameters are not
    updated in the middle of the sonification computation.

    If evaluated in a jupyter notebook, a Sonification object will be rendered
    as an ipywidget that allow the user to graphically set and update the
    parameters of the sonification.
    """

    __slots__ = '_lock', '__s'

    @final
    def __init__(
            self,
            *args,
            s: SCServer = None,
            **kwargs
    ):
        # lock making sonification operations atomic
        # we don't want values to change while process is being run
        self._lock = RLock()
        self.__s = s or scn.SC.get_default().server

        # user-defined initialization
        self.init_parameters(*args, **kwargs)

        # init_server can have side effects (such as updating the client-side
        # supercollider allocators). Accessing the resulting bundle (without any
        # side effect) will be needed when doing NRT sonification or in
        # GroupSonification
        self.init_bundle = self.init_server()
        # send init bundle to server
        self.__s.bundler().add(self.init_bundle).send()

    @property
    def s(self) -> scn.SCServer:
        """Server instance"""
        return self.__s

    @abstractmethod
    def init_parameters(self, *args, **kwargs) -> None:
        """Set initialization values for all the parameters.

        This method is called only when the object is created.
        """
        pass

    @abstractmethod
    def init_server(self) -> Bundler:
        """Return OSC messages to initialize the sonification on the server.

        Some tasks could be:
        * Send SynthDefs
        * Allocate buffers
        * Allocate busses

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

    def stop(self) -> Bundler:
        """Return OSC messages to be sent at stop time.

        Some tasks could be:
        * free all synths
        * free synths relative to this sonification
        * free a group containing all the synths relative to this sonification
        * do nothing (in case the sonification stops smoothly automatically)

        The default behaviour (in case this method is not overwritten) is that
        the default group is freed.

        :return: Bundler containing the OSC messages
        """
        with Bundler(send_on_exit=False) as bundler:
            self.s.free_all(root=False)
        return bundler

    def process(self, row: Series) -> Bundler:
        with self._lock:
            return self._process(row)

    @abstractmethod
    def _process(self, row: Series) -> Bundler:
        """Process row and return OSC messages to update the sonification.

        If the user specifies timestamps for the bundler object returned, the
        timestamps will be recalculated with respect to the execution time
        calculated by the framework. Nevertheless, the user is encouraged not to
        do so and leave the framework handle everything that concerns timing.

        :param row: pandas Series
            Data row to be sonified.
        :return: Bundler containing the OSC messages
        """
        pass

    def free(self) -> None:
        """Free server resources allocated in init_server().

        This method must be overridden if this sonification allocates resources
        for buffers or busses.

        The method returns None because the allocators are part of the client
        and no message must be sent to the server.

        This method is not called automatically at garbage collection time.
        In a jupyter notebook context, the environment keeps multiple references
        to objects that are displayed in the notebook, making automatic release
        of resources at garbage collection time not reliable.
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
    """This class allows the user to group different sonification objects.

    This will make them behave as if they were a unique sonification object.
    """

    def __init__(self, sonifications):
        for son in sonifications:
            if not isinstance(son, Sonification):
                raise ValueError(
                    f"Class {type(son)} is not a subclass of Sonification.")

        s = reduce(
            (lambda s1, s2: s1 if s1 == s2 else None),
            map(lambda son: son.s, sonifications)
        )
        if not s:
            ValueError("Not all subsonification use the same server.")

        self.s = s
        self.sonifications = sonifications

        self.init_bundle = Bundler()
        for son in sonifications:
            self.init_bundle.add(son.init_bundle)

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

    def free(self) -> None:
        for son in self.sonifications:
            son.free()

    def _ipython_display_(self):
        for son in self.sonifications:
            display(son)

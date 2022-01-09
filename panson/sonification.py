from abc import ABC, abstractmethod
import sc3nb as scn
from sc3nb.osc.osc_communication import Bundler

from functools import wraps

from pandas import Series
from threading import Lock


class Sonification(ABC):

    def __init__(self) -> None:
        # reference to default server
        self.__s = scn.SC.get_default().server
        # sync access to sonification parameters
        self._mutex = Lock()

    @property
    def _s(self) -> scn.SCServer:
        """Instance of default server"""
        return self.__s

    def get(self, item):
        with self._mutex:
            return self.__dict__[item]

    def set(self, key, value):
        with self._mutex:
            self.__dict__[key] = value

    def initialize(self):
        with self._mutex:
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
        with self._mutex:
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
        with self._mutex:
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
        with self._mutex:
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

    def __repr__(self):
        pass


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

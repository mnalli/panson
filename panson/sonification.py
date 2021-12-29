from abc import ABC, abstractmethod


class Sonification(ABC):

    def __init__(self):
        # sonification parameter mutex
        # self._mutex = "todo"

        # list of synthdefs
        self.synthdefs = None
        self.params = None

    # TODO: can be done authomatically?
    @abstractmethod
    def initialize(self):
        """Return OSC messages to initialize the sonification on the server.

        Some tasks could be:
        * Send SynthDefs

        :return: list of OSCMessage
            The order of the list is not significant.
        """
        pass

    @abstractmethod
    def start(self):
        """Return OSC messages to be sent at start time.

        Some tasks could be:
        * if working with a continuous sonification, we may want to instantiate
            the synths in advance (with amplitude 0)
        * allocate a group(s) that will contain all the synths of this
            sonification

        :return: list of OSCMessage
            The order of the list is not significant.
        """
        pass

    @abstractmethod
    def stop(self):
        """Return OSC messages to be sent at stop time.

        Some tasks could be:
        * free all synths
        * free synths relative to this sonification
        * free a group containing all the synths relative to this sonification
        * do nothing (in case the sonification stops smoothly automatically)

        :return: list of OSCMessage
            The order of the list is not significant.
        """
        pass

    @abstractmethod
    def process(self, row):
        """Process row and return OSC messages to update the sonification.

        The data row can contain information about the timing of the data, but
        the user of the framework should ignore it and leave the handling of
        timing to the framework.
        For this reason, the implementation of this method should return a
        list containing OSCMessage objects to be used for updating the
        sonification. The order of the list should not be considered.

        :param row: pandas Series
            Data row to be sonified.
        :return: list of OSCMessage
            The order of the list is not significant.
        """
        pass

    def __repr__(self):
        pass


# if __name__ == '__main__':
#     pass

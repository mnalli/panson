import matplotlib.pyplot as plt

from matplotlib.animation import FuncAnimation

import pandas as pd
from itertools import count

from math import inf

from typing import Sequence

plt.style.use('fivethirtyeight')

# TODO: refactor using draw_artists?


class FeatureDisplay:
    """This class is used to display data values in a plot.

    It is meant to be used together with DataPlayer so that the data can be
    visually navigated while the sonification is played.
    """
    # TODO
    pass


class RTFeatureDisplay:
    """This class is used to display data values in a plot.

    Data is stored in a queue, so that the newest values appear in the right
    part of the plot.

    This class can be used in both real-time and offline contexts, but, in the
    second case, the plot will show the data in the order of their execution,
    rather than in their logical ordering.
    """

    def __init__(self, labels: Sequence[str], queue_size: int = inf):
        """
        :param labels: labels to consider in the input data
        :param queue_size: size of the queue where the data is stored
            inf by default
        """
        self.x = []
        # one list for every observed key
        self.ys = {key: [] for key in labels}

        self.queue_size = queue_size
        self.index = count()

        self.fig = self.ax = None

        self.anim = None

    def show(self, fps=30) -> 'RTFeatureDisplay':
        """Show the plot.

        This will use the set matplotlib backend. For now only "notebook" is
        supported (it is the default).

        :param fps: refresh frequency of the plot
        :return: self for chaining
        """
        self.fig, self.ax = plt.subplots(1)

        # draw marker line
        self.ax.axvline(x=0, color='r', ls='-', lw=1)

        for key, y in self.ys.items():
            self.ax.plot(self.x, y, label=f'{key}')

        # store into field to avoid garbage collection of the animation
        self.anim = FuncAnimation(self.fig, self._animate, interval=1000/fps, cache_frame_data=False)

        # add automatic padding to plot
        self.ax.legend(loc='upper left')

        plt.tight_layout()
        plt.show()

        return self

    def _animate(self, i):
        """This method is executed by the thread that updates the plot."""
        self.ax.clear()

        # draw marker line
        self.ax.axvline(x=0, color='r', ls='-', lw=1)

        for key, y in self.ys.items():
            self.ax.plot(self.x, y, label=f'{key}')

        # display legend in static location
        self.ax.legend(loc='upper left')

        plt.tight_layout()

    def feed(self, row: pd.Series) -> None:
        """Feed data sample into the feature display.

        :param row: data sample as pandas Series
        """

        # TODO: better to do head insert and tail remove?

        for key, y in self.ys.items():
            y.append(row[key])

        if len(self.x) > self.queue_size:
            for key, y in self.ys.items():
                # remove oldest value
                y.pop(0)
        else:
            # head insert
            self.x.insert(0, -next(self.index))

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

import pandas as pd
from itertools import count

from IPython import get_ipython

from math import inf

ipython = get_ipython()

if ipython:
    # use "notebook" backend by default if we are in jupyter
    # TODO: can the user set to other backends?
    ipython.run_line_magic('matplotlib', 'notebook')

plt.style.use('fivethirtyeight')

# TODO: refactor using draw_artists?


class LiveFeatureDisplay:

    def __init__(self, keys, queue_size=inf):
        self.x = []
        # one list for every observed key
        self.ys = {key: [] for key in keys}

        self.queue_size = queue_size
        self.index = count()

        self.fig = self.ax = None

        self.anim = None

    def show(self, fps=30):

        self.fig, self.ax = plt.subplots(1)

        # draw marker line
        self.ax.axvline(x=0, color='r', ls='-', lw=1)

        for key, y in self.ys.items():
            self.ax.plot(self.x, y, label=f'{key}')

        # store into field to avoid garbage collection of the animation
        self.anim = FuncAnimation(self.fig, self.animate, interval=1000/fps)

        # add automatic padding to plot
        self.ax.legend(loc='upper left')

        plt.tight_layout()
        plt.show()

        return self

    def feed(self, features: pd.Series):

        # TODO: better to do head insert and tail remove?

        for key, y in self.ys.items():
            y.append(features[key])

        if len(self.x) > self.queue_size:
            for key, y in self.ys.items():
                # remove oldest value
                y.pop(0)
        else:
            # head insert
            self.x.insert(0, -next(self.index))

    def animate(self, i):
        self.ax.clear()

        # draw marker line
        self.ax.axvline(x=0, color='r', ls='-', lw=1)

        for key, y in self.ys.items():
            self.ax.plot(self.x, y, label=f'{key}')

        # display legend in static location
        self.ax.legend(loc='upper left')

        plt.tight_layout()

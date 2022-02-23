"""This module provide a set of generator functions.

This functions can be used to specify to a RTDataPlayer how to obtain data in
different contexts.

As these functions will be called by the data player without arguments,
functools.partial can be used to specify their arguments.

Dummy functions are provided for testing purposes.
"""

import pandas as pd
import csv
from itertools import count
import time
import math


def csv_fifo_gen(fifo_path: str):
    """Read csv from a named pipe and yields it line by line.

    Yields lines as pandas Series objects.
    """

    with open(fifo_path, 'r') as fifo:
        # the reader attempts to execute fifo.readline()
        # blocks if there are no lines
        reader = csv.reader(fifo, skipinitialspace=True)

        header = next(reader)

        # the loop ends when the pipe is closed from the writing side
        for i, row in enumerate(reader):
            series = pd.Series(row, header, dtype='float', name=i)
            # series = moving_average(series)
            yield series


def dummy_sin_gen(fps=30, amp=1, timestamps=True):
    """Yields sinusoidal values varying with time."""

    header = ['value']
    if timestamps:
        # head insert
        header.insert(0, 'timestamp')

    t0 = time.time()

    for i in count():
        t = time.time() - t0
        value = math.sin(t) * amp
        data = [value]
        if timestamps:
            data.insert(0, t)
        # TODO: remove explicit name
        yield pd.Series(data, header, name=i)
        # TODO: improve timing
        time.sleep(1 / fps)


def dummy_sin_cos_gen(fps=30, sin_amp=1, cos_amp=1, timestamps=True):
    """Yields oscillatory values varying with time."""

    header = ['sin', 'cos']
    if timestamps:
        # head insert
        header.insert(0, 'timestamp')
    t0 = time.time()

    for i in count():
        t = time.time() - t0
        sin = math.sin(t) * sin_amp
        cos = math.cos(t) * cos_amp
        data = [sin, cos]
        if timestamps:
            data.insert(0, t)
        # TODO: remove explicit name
        yield pd.Series(data, header, name=i)
        # TODO: improve timing
        time.sleep(1 / fps)

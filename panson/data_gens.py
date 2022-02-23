
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


def dummy_sin_gen(fps=30, amp=1):
    """Yields sinusoidal values varying with time."""

    header = ['timestamp', 'value']
    t0 = time.time()

    for i in count():
        t = time.time() - t0
        value = math.sin(t) * amp
        # TODO: remove explicit name
        yield pd.Series([t, value], header, name=i)
        # TODO: improve timing
        time.sleep(1 / fps)


def dummy_sin_cos_gen(fps=30, sin_amp=1, cos_amp=1):
    """Yields oscillatory values varying with time."""

    header = ['timestamp', 'sin', 'cos']
    t0 = time.time()

    for i in count():
        t = time.time() - t0
        sin = math.sin(t) * sin_amp
        cos = math.cos(t) * cos_amp
        # TODO: remove explicit name
        yield pd.Series([t, sin, cos], header, name=i)
        # TODO: improve timing
        time.sleep(1 / fps)


import pandas as pd
import csv
import time

from math import sin


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

    t0 = time.time()

    while True:
        delta = time.time() - t0
        yield sin(delta) * amp
        # TODO: improve timing
        time.sleep(1 / fps)

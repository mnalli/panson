import csv
import time
import math

from typing import Generator, final


class Stream:

    def __init__(self, name, datagen=None, dtype=None, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}

        # TODO: use names
        self.name = name
        self.dtype = dtype
        self._datagen = datagen

        self._args = args
        self._kwargs = kwargs

    def datagen(self, *args, **kwargs) -> Generator:
        if self._datagen:
            return self._datagen(*args, **kwargs)

        raise ValueError("Define datagen constructor argument or override datagen method.")

    @final
    def open(self) -> Generator:
        return self.datagen(*self._args, **self._kwargs)


class CsvFifo(Stream):

    @staticmethod
    def datagen(fifo_path: str) -> Generator:
        """Read csv from a named pipe and yields it line by line.

        Yields lines as pandas Series objects.
        """
        with open(fifo_path, 'r') as fifo:
            # the reader attempts to execute fifo.readline()
            # blocks if there are no lines
            reader = csv.reader(fifo, skipinitialspace=True)

            # yield header list
            yield next(reader)

            # the loop ends when the pipe is closed from the writing side
            for row in reader:
                yield row


class DummySin(Stream):

    @staticmethod
    def datagen(fps=30, amp=1, timestamps=True) -> Generator:
        """Yields sinusoidal values varying with time."""

        header = ['value']
        if timestamps:
            # head insert
            header.insert(0, 'timestamp')

        yield header

        t0 = time.time()

        while True:
            t = time.time() - t0
            value = math.sin(t) * amp
            data = [value]
            if timestamps:
                data.insert(0, t)

            yield data

            # TODO: improve timing
            time.sleep(1 / fps)


class DummySinCos(Stream):

    @staticmethod
    def datagen(fps=30, sin_amp=1, cos_amp=1, timestamps=True) -> Generator:
        """Yields oscillatory values varying with time."""

        header = ['sin', 'cos']
        if timestamps:
            # head insert
            header.insert(0, 'timestamp')

        yield header

        t0 = time.time()

        while True:
            t = time.time() - t0
            sin = math.sin(t) * sin_amp
            cos = math.cos(t) * cos_amp
            data = [sin, cos]
            if timestamps:
                data.insert(0, t)

            yield data

            # TODO: improve timing
            time.sleep(1 / fps)

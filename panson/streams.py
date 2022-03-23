import csv
import time
import math

import numpy as np

from typing import Generator, final

from typing import Any, Callable, Tuple

import logging
_LOGGER = logging.getLogger(__name__)


class Stream:

    def __init__(self, name: str, datagen=None, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}

        self.name = name

        self._datagen = datagen

        self._args = args
        self._kwargs = kwargs

        # validate generator arguments
        self.datagen(*args, **kwargs)

        # hooks
        self._open_hooks: list[Tuple[Callable[..., None], Any, Any]] = []
        self._close_hooks: list[Tuple[Callable[..., None], Any, Any]] = []

    def datagen(self, *args, **kwargs) -> Generator:
        if self._datagen:
            return self._datagen(*args, **kwargs)

        raise ValueError("Define datagen constructor argument or override datagen method.")

    @final
    def open(self) -> Generator:
        return self.datagen(*self._args, **self._kwargs)

    def add_open_hook(self, hook: Callable[..., None], *args, **kwargs):
        self._open_hooks.append((hook, args, kwargs))
        return self

    def add_close_hook(self, hook: Callable[..., None], *args, **kwargs):
        self._close_hooks.append((hook, args, kwargs))
        return self

    @staticmethod
    def _exec_hooks(hooks: list[Tuple[Callable[..., None], Any, Any]]):
        for hook, args, kwargs in hooks:
            if args and kwargs:
                hook(*args, **kwargs)
            elif args:
                hook(*args)
            elif kwargs:
                hook(**kwargs)
            else:
                hook()

    def exec_open_hooks(self):
        _LOGGER.debug(f"stream {self.name}: execute open hooks")
        self._exec_hooks(self._open_hooks)

    def exec_close_hooks(self):
        _LOGGER.debug(f"stream {self.name}: execute close hooks")
        self._exec_hooks(self._close_hooks)


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

            # yield header
            yield np.array(next(reader), dtype=str)

            # the loop ends when the pipe is closed from the writing side
            for row in reader:
                # convert strings into floats
                yield np.array(row, dtype='float64')


class DummySin(Stream):

    @staticmethod
    def datagen(fps=30, amp=1, timestamps=True) -> Generator:
        """Yields sinusoidal values varying with time."""

        header = ['value']
        if timestamps:
            # head insert
            header.insert(0, 'timestamp')

        yield np.array(header)

        t0 = time.time()

        while True:
            t = time.time() - t0
            value = math.sin(t) * amp
            data = [value]
            if timestamps:
                data.insert(0, t)

            yield np.array(data)

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

        yield np.array(header)

        t0 = time.time()

        while True:
            t = time.time() - t0
            sin = math.sin(t) * sin_amp
            cos = math.cos(t) * cos_amp
            data = [sin, cos]
            if timestamps:
                data.insert(0, t)

            yield np.array(data)

            # TODO: improve timing
            time.sleep(1 / fps)

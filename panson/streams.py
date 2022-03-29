import csv
import time
import math

import numpy as np

from typing import Generator, final

from typing import Any, Callable, Tuple

import logging

import pandas as pd

_LOGGER = logging.getLogger(__name__)


class Stream:

    def __init__(self, name: str, datagen=None, preprocessing=None, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}

        self.name = name

        self._datagen = datagen
        self._preprocessing = preprocessing

        self._args = args
        self._kwargs = kwargs

        # validate generator arguments
        self.datagen(*args, **kwargs)

        # hooks
        self._open_hooks: list[Tuple[Callable[..., None], Any, Any]] = []
        self._close_hooks: list[Tuple[Callable[..., None], Any, Any]] = []

        # set by test function
        self._length = None
        self._dtype = None
        self._fps = None

    @property
    def length(self):
        if self._length is None:
            raise ValueError('Initialize length first by calling the test method.')
        else:
            return self._length

    @property
    def dtype(self):
        if self._dtype is None:
            raise ValueError('Initialize dtype first by calling the test method.')
        else:
            return self._dtype

    @property
    def fps(self):
        if self._fps is None:
            raise ValueError('Initialize fps first by calling the test method.')
        else:
            return self._fps

    @fps.setter
    def fps(self, value):
        if value <= 0:
            raise ValueError(f"fps cannot be {value}")
        self._fps = value

    @property
    def ctype(self):
        return np.ctypeslib.as_ctypes_type(self.dtype)

    def datagen(self, *args, **kwargs) -> Generator:
        if self._datagen:
            return self._datagen(*args, **kwargs)

        raise ValueError("Define datagen constructor argument or override datagen method.")

    def preprocess(self, data: pd.Series) -> None:
        if self._preprocessing:
            self._preprocessing(data)

    @final
    def open(self) -> Generator:
        return self.datagen(*self._args, **self._kwargs)

    def add_open_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'Stream':
        self._open_hooks.append((hook, args, kwargs))
        return self

    def add_close_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'Stream':
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

    def test(self, test_rows=10, print_header=False, dryrun=False) -> 'Stream':
        self.exec_open_hooks()

        gen = self.open()

        header = next(gen)

        if print_header:
            print(header)

        timestamps = []

        first_row = next(gen)
        timestamps.append(time.time())

        if len(header) == len(first_row):
            if first_row.dtype == object:
                raise ValueError('Data dtype cannot be object.')
            print(f'length: {len(first_row)}. dtype: {first_row.dtype}.')
        else:
            raise ValueError(
                f'Header length ({len(header)}) must be equal to row length ({len(first_row)}).'
            )

        for i, row in zip(range(test_rows), gen):
            timestamps.append(time.time())

            if len(first_row) != len(row):
                raise ValueError(f'row #{i+1} has length {len(row)}')
            if first_row.dtype != row.dtype:
                raise ValueError(f'row #{i+1} has dtype {row.dtype}')

        self.exec_close_hooks()

        length = len(first_row)
        dtype = first_row.dtype
        fps = 1 / np.diff(timestamps).mean()

        print('Set info on data samples:')
        print(f'    Sample length: {length}')
        print(f'    Sample dtype: {dtype} ({np.ctypeslib.as_ctypes_type(first_row.dtype)} as ctype)')
        print(f'Around {fps} fps')

        if not dryrun:
            self._length = len(first_row)
            self._dtype = first_row.dtype
            # mean fps
            self._fps = fps

        return self


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

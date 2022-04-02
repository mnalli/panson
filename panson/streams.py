import numpy as np
import pandas as pd

from typing import Generator, final, Any, Callable, Tuple, Type

from .preprocessors import Preprocessor

import csv
import time
import math

import logging

_LOGGER = logging.getLogger(__name__)


class Stream:
    """This class is used to define real-time data streams.

    We can both instantiate it by passing to it a generator function (along with
    its call arguments) or subclassing it and overriding the datagen method;
    this is similar to how threading.Thread behaves with respect to the target
    argument and the run method.

    The generator returned by the generator function, is required to return some
    numpy arrays. These arrays have to be always of the same size.
    The first array returned is required to be the header containing labels.
    """

    def __init__(
            self,
            name: str,
            datagen=None,
            args=(),
            kwargs=None,
            preprocessor: Type[Preprocessor] = None
    ):
        """
        :param name: unique name for the stream instance
        :param datagen: data generator function
        :param args: arguments to pass to datagen at invocation
        :param kwargs: keyword arguments to pass to datagen at invocation
        :param preprocessor: type of preprocessor to apply
        """
        if kwargs is None:
            kwargs = {}

        self.name = name

        self._datagen = datagen

        self._preprocessor = preprocessor
        self._preprocessor_instance = None

        self._args = args
        self._kwargs = kwargs

        # validate generator arguments
        self.datagen(*args, **kwargs)

        # hooks
        self._open_hooks: list[Tuple[Callable[..., None], Any, Any]] = []
        self._close_hooks: list[Tuple[Callable[..., None], Any, Any]] = []

        # set by test function
        self._sample_size = None
        self._dtype = None
        self._fps = None

    @property
    def sample_size(self):
        """Size of numpy array yielded by the generator."""
        if self._sample_size is None:
            raise ValueError('Initialize length first by calling the test method.')
        else:
            return self._sample_size

    @property
    def dtype(self):
        """dtype of numpy array yielded by the generator."""
        if self._dtype is None:
            raise ValueError('Initialize dtype first by calling the test method.')
        else:
            return self._dtype

    @property
    def fps(self):
        """Frame rate of the stream."""
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
        """ctype equivalent of self.dtype."""
        return np.ctypeslib.as_ctypes_type(self.dtype)

    def datagen(self, *args, **kwargs) -> Generator:
        """Generator method.

        The returned generator should yield numpy arrays all of the same size.
        The first should contain the header with the labels, while the
        followings should contain the data samples, as if they were rows of a
        pandas DataFrame.

        :param args: arguments specified in the constructor
        :param kwargs: keyword arguments specified in the constructor
        :return: Generator object
        """
        if self._datagen:
            return self._datagen(*args, **kwargs)

        raise ValueError("Define datagen constructor argument or override datagen method.")

    def _datagen_preprocessor_wrapper(self, *args, **kwargs) -> Generator:
        """Generator method that wraps datagen with preprocessing.

        :param args:
        :param kwargs:
        :return: Generator object
        """
        gen = self.datagen(*args, **kwargs)

        # TODO: check performance and optimize

        header = next(gen)
        # first row
        row = next(gen)

        # specifying a name for the series, allow the user to stack series in
        # a DataFrame in the preprocess method
        series = pd.Series(row, header, name=0)

        self._preprocessor_instance.preprocess(series)

        header = series.index
        # array with valid numpy type
        row = series.values

        yield header
        yield row

        for i, row in enumerate(gen, start=1):
            series = pd.Series(row, header, name=i)
            self._preprocessor_instance.preprocess(series)
            yield series.values

    @final
    def open(self) -> Generator:
        """Return data generator of the stream.

        :return: Generator object
        """
        if self._preprocessor is None:
            return self.datagen(*self._args, **self._kwargs)
        else:
            # create fresh preprocessor instance
            self._preprocessor_instance = self._preprocessor()
            return self._datagen_preprocessor_wrapper(*self._args, **self._kwargs)

    def add_open_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'Stream':
        """Add hook to be executed when the stream is opened.

        :param hook
        :param args
        :param kwargs
        :return: self for chaining
        """
        self._open_hooks.append((hook, args, kwargs))
        return self

    def add_close_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'Stream':
        """Add hook to be executed when the stream is closed.

        :param hook
        :param args
        :param kwargs
        :return: self for chaining
        """
        self._close_hooks.append((hook, args, kwargs))
        return self

    @staticmethod
    def _exec_hooks(hooks: list[Tuple[Callable[..., None], Any, Any]]):
        """Execute hooks.

        :param hooks
        """
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
        """Execute open hooks."""
        _LOGGER.debug(f"stream {self.name}: execute open hooks")
        self._exec_hooks(self._open_hooks)

    def exec_close_hooks(self):
        """Execute close hooks."""
        _LOGGER.debug(f"stream {self.name}: execute close hooks")
        self._exec_hooks(self._close_hooks)

    def test(self, test_samples=10, print_header=False, dryrun=False) -> 'Stream':
        """Test the stream and inspect its characteristics.

        This function can be used both for debugging the data generator and for
        setting the properties of the stream. Multi stream data players may want
        to access to these properties in a number of situations.

        :param test_samples: number of samples to generate
        :param print_header: print the header or not
        :param dryrun: print values only or set them as well
        :return: self for chaining
        """
        self.exec_open_hooks()

        gen = self.open()

        header = next(gen)

        if print_header:
            print(header)

        timestamps = []

        first_row = next(gen)
        timestamps.append(time.time())

        sample_size = len(first_row)
        dtype = first_row.dtype

        if len(header) == sample_size:
            if first_row.dtype == object:
                raise ValueError('Data dtype cannot be object.')
        else:
            raise ValueError(
                f'Header length ({len(header)}) must be equal to row length ({sample_size}).'
            )

        for i, row in zip(range(test_samples), gen):
            timestamps.append(time.time())

            if sample_size != len(row):
                raise ValueError(f'row #{i+1} has length {sample_size}')
            if dtype != row.dtype:
                raise ValueError(f'row #{i+1} has dtype {row.dtype}')

        self.exec_close_hooks()

        # mean fps
        fps = 1 / np.diff(timestamps).mean()

        print('Set info on data samples:')
        print(f'    Sample size: {sample_size}')
        print(f'    Sample dtype: {dtype} ({np.ctypeslib.as_ctypes_type(dtype)} as ctype)')
        print(f'    Around {fps} fps')

        if not dryrun:
            self._sample_size = sample_size
            self._dtype = dtype
            self._fps = fps

        return self


class CsvFifo(Stream):
    """Read csv from a named pipe and yields it line by line."""

    @staticmethod
    def datagen(fifo_path: str) -> Generator:
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

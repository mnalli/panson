import csv
import threading

import sc3nb as scn
from sc3nb import Score, Bundler
from sc3nb.sc_objects.server import ServerOptions

import numpy as np
import pandas as pd

from time import time, sleep
from threading import Thread

from .sonification import Sonification, GroupSonification
from .streams import Stream

from .live_features import LiveFeatureDisplay
from .video_players import VideoPlayer, RTVideoPlayer

from .views import DataPlayerWidgetView, RTDataPlayerWidgetView, RTDataPlayerMultiWidgetView

from typing import Union, List, Dict

from IPython.display import display

import subprocess
import os

import copy

import multiprocessing as mp

import logging
_LOGGER = logging.getLogger(__name__)

# TODO: widgets are not updated when the data player state is changed programmatically


class _DataPlayerBase:

    def __init__(
            self,
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: Union[VideoPlayer, RTVideoPlayer] = None
    ):
        self._son = sonification
        # run flag
        self._running = False
        # worker thread that computes the sonification
        self._worker = None
        # scn.Recorder object
        self._recorder = None

        self._feature_display = feature_display
        self._video_player = video_player

    @property
    def sonification(self) -> Union[Sonification, GroupSonification]:
        return self._son

    @sonification.setter
    def sonification(self, son: Union[Sonification, GroupSonification]) -> None:
        if self._running:
            # the sonification must be stopped before changing it
            raise ValueError("Cannot change sonification while playing.")
        self._son = son

    def record_start(self, path: str = 'record.wav', overwrite=False) -> None:
        # TODO: this sends the recorder definition every time.
        # can we do better?

        if self._recorder is not None:
            raise ValueError("Recorder already working.")

        if os.path.exists(path):
            if not overwrite:
                raise FileExistsError(
                    f'{path} already exists. Use overwrite=True to overwrite it.')

        self._recorder = scn.Recorder(path=path, server=self._son.s)
        # send start bundle to the server
        self._recorder.start()

    def record_stop(self) -> None:
        if self._recorder is None:
            raise ValueError("Start the recorder first!")

        # send stop bundle to the server
        self._recorder.stop()
        self._recorder = None


class DataPlayer(_DataPlayerBase):

    def __init__(
            self,
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: VideoPlayer = None
    ):
        super().__init__(sonification, feature_display, video_player)

        # playback rate
        self._rate = 1

        # load data
        self._df = self._fps = self._time_key = None
        # index of the current data point to play
        self._ptr = 0

        self._widget_view = None

    @property
    def ptr(self) -> int:
        return self._ptr

    @property
    def rate(self) -> Union[int, float]:
        return self._rate

    @rate.setter
    def rate(self, rate: Union[int, float]):
        if rate == 0:
            raise ValueError("Cannot set rate to 0.")

        if self._running:
            # restart thread with updated rate
            self.pause()
            self._rate = rate
            self.play()
        else:
            self._rate = rate

    def load(
            self,
            data: Union[str, pd.DataFrame],
            fps: Union[int, float] = None,
            time_key: str = 'timestamp'
    ) -> 'DataPlayer':
        if self._running:
            raise ValueError("Cannot load data while playing.")

        if type(data) == str:
            self._df = self._load(data)
        elif type(data) == pd.DataFrame:
            self._df = data
        else:
            raise ValueError(
                f"Cannot load {data} of type {type(data)}."
                f"Needing {pd.DataFrame} or a path to a csv file."
            )

        if self._widget_view is not None:
            self._widget_view.update_slider_max(self._df.index[-1])

        self._fps = fps
        self._ptr = 0

        if type(time_key) != str:
            raise ValueError(
                f"time_key cannot be a {type(time_key)}: must be string.")

        if fps is None:
            self._time_key = time_key
        else:
            self._time_key = None

        return self

    @staticmethod
    def _load(df_path: str) -> pd.DataFrame:
        # TODO: support all format automatically
        df = pd.read_csv(df_path, sep=r',\s*', engine='python')
        return df

    def play(self):
        if self._running:
            raise ValueError("Already playing!")

        self._worker = Thread(name='player', target=self._play)
        self._worker.start()

    def _play(self):
        assert self.rate != 0, "rate == 0"

        _LOGGER.info('player thread started')

        assert not self._running, "called while running"
        self._running = True

        # send start bundle
        self._son.s.bundler().add(self._son.start()).send()

        start_ptr = self._ptr
        t0 = time()

        if self._fps is None:
            start_timestamp = self._df.iloc[start_ptr][self._time_key]

        if self._fps:
            # TODO: refactor this variable
            visited_rows = 0
        # used to decide the direction of the iteration
        rate_sign = int(self._rate / abs(self._rate))

        # iterate over dataframe rows, from the current element on
        for ptr, row in self._df.iloc[start_ptr::rate_sign].iterrows():

            if not self._running:
                # pause was called
                break

            if self._fps:
                target_time = t0 + (visited_rows * 1 / self._fps) / abs(self._rate)
                visited_rows += 1
            else:
                target_time = t0 + (row[self._time_key] - start_timestamp) / self._rate

            # process, bundle and send
            self._son.s.bundler(target_time).add(self._son.process(row)).send()

            if self._feature_display:
                self._feature_display.feed(row)

            if self._video_player:
                if self._fps:
                    t = ptr / self._fps
                else:
                    t = row[self._time_key]

                self._video_player.seek_time(t)

            # TODO: not thread safe
            # update pointer to current row
            self._ptr = ptr

            if self._widget_view:
                self._widget_view.update_slider(self._ptr)

            # sleep for the missing time
            waiting_time = target_time - time()
            # print(waiting_time)
            if waiting_time > 0:
                sleep(waiting_time)

        # send stop bundle
        self._son.s.bundler().add(self._son.stop()).send()

        # this is relevant when the for loop ends naturally
        self._running = False
        _LOGGER.info('player thread ended')

    def pause(self) -> None:
        if not self._running:
            raise ValueError('Already paused!')
        # stop workin thread
        self._running = False
        self._worker.join()

    def seek(self, target: Union[int, float]) -> None:
        if isinstance(target, int):
            self._seek_idx(target)
        elif isinstance(target, float):
            self._seek_time(target)
        else:
            raise ValueError(
                "time must be an int (frame index) or float (seconds). "
                f"Cannot be {type(target)}."
            )

    def _seek_time(self, t: float) -> None:
        if self._fps:
            max_time = self._df.index[-1] * 1 / self._fps
            if not (0 <= t <= max_time):
                raise ValueError(
                    f"Cannot set time to {t}. "
                    f"Must be between 0 and {max_time}."
                )
            frame_idx = int(t * self._fps)
        else:
            max_time = self._df[self._time_key].iloc[-1]
            if not (0 <= t <= max_time):
                raise ValueError(
                    f"Cannot set time to {t}. "
                    f"Must be between 0 and {max_time}."
                )
            timestamps = self._df[self._time_key]
            # seek with binary search
            frame_idx = np.searchsorted(timestamps, t)

        self._seek_idx(frame_idx)

    def _seek_idx(self, idx: int) -> None:
        if not (0 <= idx <= self._df.index[-1]):
            raise ValueError(
                f"Invalid index {idx}. "
                f"Must be in range [0, {self._df.index[-1]}]"
            )

        if self._video_player:
            if self._fps:
                t = idx / self._fps
            else:
                row = self._df.iloc[idx]
                t = row[self._time_key]

            self._video_player.seek_time(t)

        if self._running:
            # restart thread with updated position
            self.pause()
            self._ptr = idx
            self.play()
        else:
            self._ptr = idx

    def _get_score(self, end_delay) -> Dict[float, List[scn.OSCMessage]]:

        # shallow copy: this works only if the user redefines object that would
        # otherwise be modified in place. The structure of the framework should
        # push the user to do so.
        clone = copy.copy(self.sonification)

        # use Bundler class to ignore server latency
        with Bundler(send_on_exit=False) as bundler:

            # if this allocates buffers or busses, the allocators will change
            # state even if the message is not sent to the server
            bundler.add(clone.init_bundle)

            # add default group
            bundler.add(clone.s.default_group.new(return_msg=True))

            bundler.add(clone.start())

            for i, row in self._df.iterrows():
                if self._fps:
                    timestamp = i / self._fps / self.rate
                else:
                    timestamp = row[self._time_key] / self.rate
                bundler.add(timestamp, clone.process(row))

            # stop sonification on last message
            # useful if stop triggers a gate
            bundler.add(timestamp, clone.stop())

            end_timestamp = timestamp + end_delay
            # close the audio file
            bundler.add(end_timestamp, "/c_set", [0, 0])

        return bundler.messages()

    def export(
            self,
            out_file: str = 'out',
            sample_rate: int = 44100,
            header_format: str = "AIFF",
            sample_format: str = "int16",
            options: ServerOptions = None,
            end_delay: float = 0.1
    ) -> subprocess.CompletedProcess:
        """Render current sonification using NRT synthesis.

        :param out_file: Path of the resulting sound file.
        :param sample_rate: sample rate for synthesis
        :param header_format: header format of the output file
        :param sample_format: sample format of the output file
        :param options: instance of server options to specify server options
        :param end_delay: time offset to add to the end of the file before
            putting the end tag
        :return: Completed scsynth non-realtime process.
        """

        score = self._get_score(end_delay)

        print(f'Rendering with rate == {self.rate}')

        return Score.record_nrt(
            score,
            "/tmp/score.osc",   # throw away score file
            out_file,
            sample_rate=sample_rate,
            header_format=header_format,
            sample_format=sample_format,
            options=options
        )

    def _ipython_display_(self):
        if self._widget_view is None:
            if self._df is None:
                max_idx = 0
            else:
                max_idx = self._df.index[-1]
            self._widget_view = DataPlayerWidgetView(self, max_idx)

        display(self._widget_view)


class DataLogger:

    def __init__(self):
        self.logging = False
        self._log_file = None
        self._writer = None
        self._first_line = False

    def start(self, path: str, overwrite: bool):
        if self.logging:
            raise ValueError("Already logging.")

        if os.path.exists(path):
            if not overwrite:
                raise FileExistsError(
                    f'{path} already exists. Use overwrite=True to overwrite it.')

        self.logging = True
        self._log_file = open(path, 'w')
        self._writer = csv.writer(self._log_file)
        self._first_line = True

    def stop(self):
        if not self.logging:
            raise ValueError("Start logger first!")

        self.logging = False

        self._log_file.close()
        self._log_file = None

        self._writer = None

    def feed(self, row: pd.Series):
        if not self.logging:
            raise ValueError('Start logger first!')

        if self._first_line:
            self._writer.writerow(row.index)
            self._first_line = False

        self._writer.writerow(row.array)


class _RTDataPlayerBase(_DataPlayerBase):

    def __init__(
            self,
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: RTVideoPlayer = None
    ):
        super().__init__(sonification, feature_display, video_player)

        self._logger = DataLogger()

        # this will be set when starting to listen
        self._t_start = None

    def listen(self):
        if self._running:
            raise ValueError("Already listening!")

        self._running = True
        self._t_start = time()

    def record_start(self, path='record.wav', overwrite=False) -> None:
        super().record_start(path, overwrite)

        if self._video_player:
            self._video_player.record(self._t_start)

    def record_stop(self) -> None:
        super().record_stop()

        if self._video_player:
            self._video_player.stop()

    def log_start(self, path='log.csv', overwrite=False) -> None:
        self._logger.start(path, overwrite)

        if self._video_player:
            self._video_player.record(self._t_start)

    def log_stop(self) -> None:
        self._logger.stop()

        if self._video_player:
            self._video_player.stop()


class RTDataPlayer(_RTDataPlayerBase):

    def __init__(
            self,
            stream: Stream,
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: RTVideoPlayer = None,
            timestamp: bool = False
    ):
        super().__init__(sonification, feature_display, video_player)

        self._stream = stream

        self._timestamp = timestamp

        # create widget only if needed (lazy)
        self._widget_view = None

    def listen(self) -> None:
        super().listen()

        self._stream.exec_open_hooks()

        self._worker = Thread(name='listener', target=self._listen)
        self._worker.start()

    def _listen(self) -> None:
        _LOGGER.info('listener thread started')

        try:
            # send start bundle
            self._son.s.bundler().add(self._son.start()).send()

            data_generator = self._stream.open()

            header = next(data_generator)
            header = pd.Index(header, dtype=str)

            if header.has_duplicates:
                raise ValueError(f'header has duplicated values - {header.duplicated()}')

            for row in data_generator:

                if not self._running:
                    # close was called
                    break

                series = pd.Series(row, header)

                if self._timestamp:
                    series['timestamp'] = time() - self._t_start

                # compute and send sonification information
                self._son.s.bundler().add(self._son.process(series)).send()

                if self._logger.logging:
                    self._logger.feed(series)

                if self._feature_display:
                    self._feature_display.feed(series)
        finally:
            # relevant when the for loop ends naturally
            self._running = False
            # send stop bundle
            self._son.s.bundler().add(self._son.stop()).send()

            self._stream.exec_close_hooks()
            _LOGGER.info('listener thread ended')

    def close(self) -> None:
        if not self._running:
            raise ValueError('Already closed!')

        # stop workin thread
        self._running = False
        self._worker.join()

    def _ipython_display_(self):
        if self._widget_view is None:
            self._widget_view = RTDataPlayerWidgetView(self)

        display(self._widget_view)


class RTDataPlayerMulti(_RTDataPlayerBase):

    def __init__(
            self,
            fps,
            streams: list[Stream],
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: RTVideoPlayer = None
    ):
        super().__init__(sonification, feature_display, video_player)

        self._fps = fps

        if len(streams) == 0:
            raise ValueError("Empty list of streams.")

        self._streams = streams

        # threads that fetch data from the streams
        self._stream_workers = None

        # slots for storing data current values
        self._stream_slots = None

        # events that are set when the stream generates the first data sample
        self._first_sample_events = None

        self._stream_loggers = [DataLogger() for _ in self._streams]

        # create widget only if needed (lazy)
        self._widget_view = None

    @property
    def streams(self) -> list[Stream]:
        return self._streams

    def listen(self) -> None:
        super().listen()

        # execute all opening hooks sequentially, not to mix their output
        for stream in self._streams:
            stream.exec_open_hooks()

        self._worker = Thread(name='listener', target=self._listen)
        # allocate one thread for each stream
        self._stream_workers = [
            Thread(
                name=f'{stream.name}-thread',
                target=self._stream,
                args=(i,)
            ) for i, stream in enumerate(self._streams)
        ]

        # allocate slots' memory
        self._stream_slots = [None] * len(self._streams)

        # one first sample event for stream
        self._first_sample_events = [threading.Event() for _ in self._streams]

        # start stream threads
        for thread in self._stream_workers:
            thread.start()

        self._worker.start()

    def _listen(self) -> None:
        _LOGGER.info('sonification thread started')

        try:
            # send start bundle
            self._son.s.bundler().add(self._son.start()).send()

            # wait for every stream to start producing data
            for event in self._first_sample_events:
                event.wait()

            i = 1
            t0 = time()

            while self._running:

                if i == 1:
                    # verify integrity only on the first iteration
                    row = pd.concat(self._stream_slots, verify_integrity=True, copy=False)
                else:
                    row = pd.concat(self._stream_slots, copy=False)

                # mandatory timestamp in case of multiple streams
                # the shared start time is used as reference
                row['timestamp'] = time() - self._t_start

                self._son.s.bundler().add(self._son.process(row)).send()

                # for timestamp in row[[f'{stream.name}_timestamp' for stream in self._streams]]:
                #     _LOGGER.debug(f'{row["timestamp"]}:{timestamp - row["timestamp"]}')

                if self._logger.logging:
                    self._logger.feed(row)

                if self._feature_display:
                    self._feature_display.feed(row)

                target_time = t0 + i / self._fps
                i += 1

                waiting_time = target_time - time()

                if waiting_time > 0:
                    sleep(waiting_time)
                else:
                    _LOGGER.warning(f'Thread {-waiting_time} s late')

        finally:
            self._running = False
            # send stop bundle
            self._son.s.bundler().add(self._son.stop()).send()
            _LOGGER.info('sonification thread ended')

    def _stream(self, idx: int):
        assert 0 <= idx < len(self._streams)

        stream = self._streams[idx]
        _LOGGER.info(f'stream {stream.name} opened')

        try:
            data_generator = stream.open()

            header = next(data_generator)
            header = pd.Index(header, dtype=str)

            if header.has_duplicates:
                raise ValueError(f'stream {stream.name}: header has duplicated values - {header.duplicated()}')

            # get first data sample
            row = next(data_generator)

            series = pd.Series(row, header)

            # add stream timestamp
            # TODO: does this resize the array?
            series[f'{stream.name}_timestamp'] = time() - self._t_start

            self._stream_slots[idx] = series

            # signal event to main thread
            self._first_sample_events[idx].set()

            for row in data_generator:

                if not self._running:
                    break

                series = pd.Series(row, header)

                # add stream timestamp
                series[f'{stream.name}_timestamp'] = time() - self._t_start

                self._stream_slots[idx] = series

                if self._stream_loggers[idx].logging:
                    self._stream_loggers[idx].feed(series)
        finally:
            self._running = False
            stream.exec_close_hooks()
            _LOGGER.info(f'stream {stream.name} closed')

    def close(self) -> None:
        if not self._running:
            raise ValueError('Already closed!')

        # stop workin thread
        self._running = False

        self._worker.join()

        for thread in self._stream_workers:
            thread.join()

    def log_start_stream(self, idx: int, path=None, overwrite=False) -> None:
        self._stream_loggers[idx].start(path, overwrite)

    def log_stop_stream(self, idx: int) -> None:
        self._stream_loggers[idx].stop()

    def _ipython_display_(self):
        if self._widget_view is None:
            self._widget_view = RTDataPlayerMultiWidgetView(self)

        display(self._widget_view)


class RTDataPlayerMultiParallel(_RTDataPlayerBase):

    def __init__(
            self,
            fps,
            streams: list[Stream],
            sonification: Union[Sonification, GroupSonification],
            feature_display: LiveFeatureDisplay = None,
            video_player: RTVideoPlayer = None
    ):
        super().__init__(sonification, feature_display, video_player)

        self._fps = fps

        if len(streams) == 0:
            raise ValueError("Empty list of streams.")

        self._streams = streams

        # processes that fetch data from streams
        self._stream_processes = None

        # shared memory slots for storing newest values of streams
        self._stream_slots = None
        # pipe connections to communicate with children processes
        self._pipes = None

        # self._stream_loggers = [DataLogger() for _ in self._streams]

        # create widget only if needed (lazy)
        self._widget_view = None

    @property
    def streams(self) -> list[Stream]:
        return self._streams

    def listen(self) -> None:
        super().listen()

        # execute all opening hooks sequentially, not to mix their output
        for stream in self._streams:
            stream.exec_open_hooks()

        # the sonification is computed by a thread of the current process
        self._worker = Thread(name='listener', target=self._listen)

        self._stream_processes = []
        self._stream_slots = []
        self._pipes = []

        for i, stream in enumerate(self._streams):
            conn, child_conn = mp.Pipe()
            self._pipes.append(conn)

            # +1 is for the timestamp
            # TODO: timestamp is always a float
            slot = mp.Array(stream.ctype, stream.length + 1)
            self._stream_slots.append(slot)

            proc = mp.Process(
                target=self._stream,
                args=(self._t_start, self._streams[i], slot, child_conn)
            )
            self._stream_processes.append(proc)

        # start stream processes
        for process in self._stream_processes:
            process.start()

        self._worker.start()

    def _listen(self) -> None:
        _LOGGER.info('sonification process started')

        try:
            # send start bundle
            self._son.s.bundler().add(self._son.start()).send()

            # receive indexes
            headers = [conn.recv() for conn in self._pipes]
            header = pd.Index([]).append(headers)

            if header.has_duplicates:
                raise ValueError(f'sonification process: merged header has duplicated values - {header.duplicated()}')

            for conn in self._pipes:
                # read start message from every pipe
                msg = conn.recv()
                assert msg == 'start'

            i = 1
            t0 = time()

            while self._running:

                # *copy* and store slots in local memory
                stream_slots = [
                    np.array(slot, dtype=stream.dtype) for stream, slot in zip(self.streams, self._stream_slots)
                ]

                # TODO: what to do if streams have different types???
                # check *casting* argument
                row = np.concatenate(stream_slots)

                series = pd.Series(row, header)

                # mandatory timestamp in case of multiple streams
                # the shared start time is used as reference
                series['timestamp'] = time() - self._t_start

                self._son.s.bundler().add(self._son.process(series)).send()

                # for timestamp in row[[f'{stream.name}_timestamp' for stream in self._streams]]:
                #     _LOGGER.debug(f'{row["timestamp"]}:{timestamp - row["timestamp"]}')

                if self._logger.logging:
                    self._logger.feed(series)

                if self._feature_display:
                    self._feature_display.feed(series)

                target_time = t0 + i / self._fps
                i += 1

                waiting_time = target_time - time()

                if waiting_time > 0:
                    sleep(waiting_time)
                else:
                    _LOGGER.warning(f'Thread {-waiting_time} s late')

        finally:
            # send stop bundle
            self._son.s.bundler().add(self._son.stop()).send()
            # send stop message to every process
            for conn in self._pipes:
                conn.send('stop')

            _LOGGER.info('sonification thread ended')

    @staticmethod
    def _stream(t_start, stream: Stream, slot: mp.Array, conn: mp.connection.Connection):

        _LOGGER.info(f'stream {stream.name} opened')

        try:
            data_generator = stream.open()

            header = next(data_generator)
            # insert timestamp label at head position
            # np.insert does not resize string types of the array
            header = np.concatenate(([f'{stream.name}_timestamp'], header))

            header = pd.Index(header, dtype=str)

            if header.has_duplicates:
                raise ValueError(f'stream {stream.name}: header has duplicated values - {header.duplicated()}')

            # send header
            conn.send(header)

            # get first data sample
            row = next(data_generator)

            # stream timestamp
            timestamp = time() - t_start

            slot[0] = timestamp
            slot[1:] = row

            # signal event to main thread
            conn.send('start')

            for row in data_generator:

                while conn.poll():
                    msg = conn.recv()
                    if msg == 'stop':
                        # finally is executed
                        return

                # stream timestamp
                timestamp = time() - t_start

                slot[0] = timestamp
                slot[1:] = row

                # if self._stream_loggers[idx].logging:
                #     self._stream_loggers[idx].feed(series)
        finally:
            conn.send('exit')
            stream.exec_close_hooks()
            _LOGGER.info(f'stream {stream.name} closed')

    def close(self) -> None:
        if not self._running:
            raise ValueError('Already closed!')

        # stop workin thread
        self._running = False

        self._worker.join()

        for proc in self._stream_processes:
            proc.join()

    # def log_start_stream(self, idx: int, path=None, overwrite=False) -> None:
    #     self._stream_loggers[idx].start(path, overwrite)

    # def log_stop_stream(self, idx: int) -> None:
    #     self._stream_loggers[idx].stop()

    def _ipython_display_(self):
        if self._widget_view is None:
            self._widget_view = RTDataPlayerMultiWidgetView(self)

        display(self._widget_view)

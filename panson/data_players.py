import sc3nb as scn
from sc3nb import Score, Bundler

import pandas as pd

from time import time, sleep
from threading import Thread, Lock

from .sonification import Sonification

import logging
_LOGGER = logging.getLogger(__name__)


class DataPlayer:

    def __init__(self, sonification=None, data=None, fps=None):
        self._son = sonification

        # reference to default server
        self._s = scn.SC.get_default().server

        self._recorder = None

        # worker thread
        self._worker = None

        # running info and lock
        self._running = False
        self._running_lock = Lock()

        # load data
        self._df = self._fps = None
        # index of the current data point to play
        self._ptr = 0

        if data is not None:
            self.load(data, fps)

    @property
    def sonification(self):
        return self._son

    @sonification.setter
    def sonification(self, son):
        if not issubclass(son, Sonification):
            raise ValueError(f"Cannot assign a {type(son)} object as sonification")

        with self._running_lock:
            if self._running:
                # the sonification must be stopped before changing it
                raise ValueError("Cannot change sonification while playing.")
        self._son = son

    def load(self, data, fps=None):
        with self._running_lock:
            if self._running:
                raise ValueError("Cannot load data while playing.")

        if type(data) == type(str):
            self._df = self._load(data)
        elif type(data) == pd.DataFrame:
            self._df = data
        else:
            raise ValueError(
                f"Cannot load {data} of type {type(data)}."
                f"Needing {pd.DataFrame} or a path to a csv file."
            )
        # TODO: check fps type
        self._fps = fps
        self._ptr = 0

    @staticmethod
    def _load(df_path):
        # TODO: support all format automatically
        df = pd.read_csv(df_path, sep=r',\s*', engine='python')
        return df

    def play(self, rate=1):
        if rate == 0:
            raise ValueError("Cannot use a rate of 0.")

        with self._running_lock:
            if self._running:
                raise ValueError("Already playing!")

        self._worker = Thread(name='player', target=self._play, args=(rate,))
        self._worker.start()

    def _play(self, rate):
        assert rate != 0, "rate == 0"

        _LOGGER.info('player thread starting')

        with self._running_lock:
            assert not self._running, "called while running"
            self._running = True

        # TODO: do it every time???
        # load synthdefs on the server
        self._s.bundler().add(self._son.initialize()).send()

        # TODO: is it better to instantiate asap?
        self._s.bundler().add(self._son.start()).send()

        # assumpions:
        #   timestamp contains time info
        #   positive rate

        start_ptr = self._ptr
        t0 = time()

        if self._fps is None:
            start_timestamp = self._df.iloc[start_ptr].timestamp

        i = 0

        # iterate over dataframe rows, from the current element on
        for _, row in self._df.iloc[start_ptr:-1].iterrows():
            with self._running_lock:
                if not self._running:
                    # pause was called
                    break

            if self._fps:
                target_time = t0 + (i * 1 / self._fps) / rate
            else:
                target_time = t0 + (row.timestamp - start_timestamp) / rate

            i += 1

            # process, bundle and send
            self._s.bundler(target_time).add(self._son.process(row)).send()

            self._ptr += 1

            # sleep for the missing time
            waiting_time = target_time - time()
            # print(waiting_time)
            if waiting_time > 0:
                sleep(waiting_time)

        # send stop bundle
        self._s.bundler().add(self._son.stop()).send()

        # this is relevant when the for loop ends naturally
        with self._running_lock:
            self._running = False
        _LOGGER.info('player thread exiting')

    def pause(self):
        with self._running_lock:
            if not self._running:
                raise ValueError('Already paused!')
            # stop workin thread
            self._running = False
        self._worker.join()

    # TODO: time input
    def seek(self, idx):
        if not (0 <= idx <= self._df.index[-1]):
            raise ValueError(
                f"Invalid index {idx}."
                f"Must be in range [0, {self._df.index[-1]}]"
            )

        with self._running_lock:
            local_running = self._running

        if local_running:
            # restart thread with updated position
            self.pause()
            self._ptr = idx
            # TODO: start with same rate
            self.play()
        else:
            self._ptr = idx

    # TODO: the code in RTDataPlayer is duplicated...
    def record_start(self, path='record.wav'):
        # TODO: this sends the recorder definition every time.
        # can we do better?

        if self._recorder is not None:
            raise ValueError("Recorder already working.")

        self._recorder = scn.Recorder(path=path, server=self._s)
        # send start bundle to the server
        self._recorder.start()

    def record_stop(self):
        if self._recorder is None:
            raise ValueError("Start the recorder first!")

        # send stop bundle to the server
        self._recorder.stop()
        self._recorder = None

    def _get_score(self):
        # use Bundler class to ignore server latency
        with Bundler(send_on_exit=False) as bundler:
            # load synthdefs on NRT server
            bundler.add(self.sonification.initialize())

            # add default group
            bundler.add(self._s.default_group.new(return_msg=True))

            # instantiate synths
            bundler.add(self.sonification.start())

            # iterate over dataframe rows
            for _, row in self._df.iterrows():
                # lend over row (pd.Series) to Sonification
                bundler.add(row.timestamp, self.sonification.process(row))

            # TODO: call sonification.stop???

            # TODO: this way the last line will not count? do we want to add an offset?
            # /c_set [0, 0] will close the audio file
            bundler.add(row.timestamp, "/c_set", [0, 0])

        return bundler.messages()

    # TODO: export a precise range?
    def export(self, out_path):
        score = self._get_score()

        # TODO: support other headers and scorefile paths?
        Score.record_nrt(score, "/tmp/score.osc", out_path, header_format="WAV")

    # def __repr__(self):
    #     pass


# TODO: add listening hooks
# TODO: add closing hooks

class RTDataPlayer:

    def __init__(self, datagen_function, sonification=None):
        self.sonification = sonification
        self._datagen = datagen_function
        self._s = scn.SC.get_default().server

        # worker thread
        self._worker = None

        # running info and lock
        self._running = False
        self._running_lock = Lock()

        self._recorder = None

        # logs dataframe and lock
        self._logs = None
        self._logs_lock = Lock()

    def listen(self):
        with self._running_lock:
            if self._running:
                raise ValueError("Already listening!")

        self._worker = Thread(name='listener', target=self._listen)
        self._worker.start()

    def _listen(self):
        _LOGGER.info('listener thread starting')

        with self._running_lock:
            self._running = True

        # TODO: do it every time???
        # load synthdefs on the server
        self._s.bundler().add(self.sonification.initialize()).send()

        # TODO: is it better to instantiate asap?
        self._s.bundler().add(self.sonification.start()).send()

        for row in self._datagen():
            with self._running_lock:
                # close was called
                if not self._running:
                    break

            self._s.bundler().add(self.sonification.process(row)).send()

            with self._logs_lock:
                # if logging is enabled, log the data
                if self._logs is not None:
                    # TODO: handle out of memory error
                    self._logs = self._logs.append(row)

        # send stop bundle
        self._s.bundler().add(self.sonification.stop()).send()

        # this is relevant when the for loop ends naturally
        with self._running_lock:
            self._running = False
        _LOGGER.info('listener thread exiting')

    def close(self):
        with self._running_lock:
            if not self._running:
                raise ValueError('Already closed!')
            # stop workin thread
            self._running = False
        self._worker.join()

    def record_start(self, path='record.wav'):
        # TODO: this sends the recorder definition every time.
        # can we do better?

        if self._recorder is not None:
            raise ValueError("Recorder already working.")

        self._recorder = scn.Recorder(path=path, server=self._s)
        # send start bundle to the server
        self._recorder.start()

    def record_stop(self):
        if self._recorder is None:
            raise ValueError("Start the recorder first!")

        # send stop bundle to the server
        self._recorder.stop()
        self._recorder = None

    def log_start(self):
        with self._logs_lock:
            if self._logs is not None:
                raise ValueError("Already logging.")
            self._logs = pd.DataFrame()

    def log_stop(self):
        with self._logs_lock:
            if self._logs is None:
                raise ValueError("Start logging first!")
            df = self._logs
            self._logs = None

        return df

import sc3nb as scn
from sc3nb import Score, Bundler

import pandas as pd

from time import time, sleep
from threading import Thread

from .sonification import Sonification
from .live_features import LiveFeatureDisplay

from typing import Union, Any, Callable, Generator, List, Tuple, Dict

import ipywidgets as widgets
from IPython.display import display

import logging
_LOGGER = logging.getLogger(__name__)


class DataPlayer:

    def __init__(
            self,
            sonification: Sonification = None,
            feature_display: LiveFeatureDisplay = None
    ) -> None:
        # reference to default server
        self._s = scn.SC.get_default().server

        # worker thread
        self._worker = None
        # run flag
        self._running = False
        # playback rate
        self._rate = 1

        self._son = None
        if sonification:
            # set sonification and initialize
            self.sonification = sonification

        self._recorder = None

        # load data
        self._df = self._fps = self._time_key = None
        # index of the current data point to play
        self._ptr = 0

        self._widget = self._get_ipywidget()
        self._feature_display = feature_display

    @property
    def sonification(self) -> Sonification:
        return self._son

    @sonification.setter
    def sonification(self, son: Sonification) -> None:
        if not isinstance(son, Sonification):
            raise ValueError(f"Cannot assign a {type(son)} object as sonification.")

        if self._running:
            # the sonification must be stopped before changing it
            raise ValueError("Cannot change sonification while playing.")

        self._son = son

        # load synthdefs on the server
        self._s.bundler().add(self._son.initialize()).send()

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

        self._widget.children[0].max = self._df.index[-1]

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

        _LOGGER.info('player thread starting')

        assert not self._running, "called while running"
        self._running = True

        # send start bundle
        self._s.bundler().add(self._son.start()).send()

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
            self._s.bundler(target_time).add(self._son.process(row)).send()

            if self._feature_display:
                self._feature_display.feed(row)

            # TODO: not thread safe
            # update pointer to current row
            self._ptr = ptr
            slider = self._widget.children[0]
            # TODO: refactor
            callback = slider._trait_notifiers['value']['change'][0]

            slider.unobserve(callback, 'value')
            slider.value = self._ptr
            slider.observe(callback, 'value')

            # sleep for the missing time
            waiting_time = target_time - time()
            # print(waiting_time)
            if waiting_time > 0:
                sleep(waiting_time)

        # send stop bundle
        self._s.bundler().add(self._son.stop()).send()

        # this is relevant when the for loop ends naturally
        self._running = False
        _LOGGER.info('player thread exiting')

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

    def _seek_time(self, time: float) -> None:
        if self._fps:
            max_time = self._df.index[-1] * 1 / self._fps
            if not (0 <= time <= max_time):
                raise ValueError(
                    f"Cannot set time to {time}. "
                    f"Must be between 0 and {max_time}."
                )
            frame_idx = int(time * self._fps)
        else:
            max_time = self._df[self._time_key].iloc[-1]
            if not (0 <= time <= max_time):
                raise ValueError(
                    f"Cannot set time to {time}. "
                    f"Must be between 0 and {max_time}."
                )
            timestamps = self._df[self._time_key]
            # pick index nearest to the timestamp
            # TODO: accelerate forward searching in monotonous functions
            frame_idx = ((timestamps - time).abs()).argmin()

        self._seek_idx(frame_idx)

    def _seek_idx(self, idx: int) -> None:
        if not (0 <= idx <= self._df.index[-1]):
            raise ValueError(
                f"Invalid index {idx}. "
                f"Must be in range [0, {self._df.index[-1]}]"
            )

        if self._running:
            # restart thread with updated position
            self.pause()
            self._ptr = idx
            self.play()
        else:
            self._ptr = idx

    # TODO: the code in RTDataPlayer is duplicated...
    def record_start(self, path: str = 'record.wav') -> None:
        # TODO: this sends the recorder definition every time.
        # can we do better?

        if self._recorder is not None:
            raise ValueError("Recorder already working.")

        self._recorder = scn.Recorder(path=path, server=self._s)
        # send start bundle to the server
        self._recorder.start()

    def record_stop(self) -> None:
        if self._recorder is None:
            raise ValueError("Start the recorder first!")

        # send stop bundle to the server
        self._recorder.stop()
        self._recorder = None

    # TODO: export with rate
    def _get_score(self) -> Dict[float, List[scn.OSCMessage]]:
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
                bundler.add(row[self._time_key], self.sonification.process(row))

            # TODO: call sonification.stop???

            # TODO: this way the last line will not count? do we want to add an offset?
            # /c_set [0, 0] will close the audio file
            bundler.add(row[self._time_key], "/c_set", [0, 0])

        return bundler.messages()

    # TODO: export a precise range?
    def export(self, out_path: str = 'out.wav') -> None:
        score = self._get_score()

        # TODO: support other headers and scorefile paths?
        Score.record_nrt(score, "/tmp/score.osc", out_path, header_format="WAV")

    def _get_ipywidget(self):

        slider = widgets.IntSlider(
            value=self._ptr,
            min=0,
            # TODO: if the data is not loaded
            # max=max_idx,
            layout=widgets.Layout(width='98%'),
            # continuous_update=False
        )

        beginning = widgets.Button(icon='fast-backward')
        end = widgets.Button(icon='fast-forward')

        backward = widgets.Button(icon='step-backward')
        forward = widgets.Button(icon='step-forward')

        pause = widgets.Button(icon='pause')
        play = widgets.Button(icon='play')

        record = widgets.ToggleButton(
            value=False,
            description='Record',
            icon='microphone'
        )
        record_out = widgets.Text(
            value='record.wav',
            description='Output path:',
        )
        # TODO: overwrite check button
        record_box = widgets.HBox([record, record_out])

        clear_out = widgets.Button(
            description='Clear output'
        )

        out = widgets.Output(layout={'border': '1px solid black'})

        controls = widgets.HBox([
            beginning,
            backward,
            pause,
            play,
            forward,
            end
        ])
        widget = widgets.VBox([
            slider,
            controls,
            record_box,
            clear_out,
            out
        ])

        # bind callbacks

        def on_change(value):
            with out:
                self._seek_idx(value['new'])

        slider.observe(on_change, 'value')

        def on_beginning(button):
            with out:
                slider.value = 0

        def on_end(button):
            with out:
                slider.value = self._df.index[-1]

        beginning.on_click(on_beginning)
        end.on_click(on_end)

        # TODO: atomicity?
        def on_backward(button):
            with out:
                slider.value -= 10

        def on_forward(button):
            with out:
                slider.value += 10

        backward.on_click(on_backward)
        forward.on_click(on_forward)

        def on_pause(button):
            with out:
                self.pause()

        def on_play(button):
            with out:
                self.play()

        pause.on_click(on_pause)
        play.on_click(on_play)

        def on_clear(button):
            out.clear_output()

        clear_out.on_click(on_clear)

        return widget

    def _ipython_display_(self):
        display(self._widget)


class RTDataPlayer:

    def __init__(
            self,
            datagen_function: Callable[[], Generator],
            sonification: Sonification = None,
            feature_display: LiveFeatureDisplay = None
    ) -> None:
        self._s = scn.SC.get_default().server

        self._datagen = datagen_function

        # worker thread
        self._worker = None
        # run flag
        self._running = False

        self._son = None
        if sonification:
            # set sonification and initialize
            self.sonification = sonification

        self._recorder = None

        # logs dataframe and lock
        self._logs = None

        # hooks
        self._listen_hooks: List[Tuple[Callable[..., None], Any, Any]] = []
        self._close_hooks:  List[Tuple[Callable[..., None], Any, Any]] = []

        # create widget only once
        self._widget = self._get_ipywidget()

        self._feature_display = feature_display

    @property
    def sonification(self) -> Sonification:
        return self._son

    @sonification.setter
    def sonification(self, son: Sonification) -> None:
        if not isinstance(son, Sonification):
            raise ValueError(f"Cannot assign a {type(son)} object as sonification.")

        if self._running:
            # the sonification must be stopped before changing it
            raise ValueError("Cannot change sonification while playing.")

        self._son = son
        
        # load synthdefs on the server
        self._s.bundler().add(self._son.initialize()).send()

    def listen(self) -> None:
        if self._running:
            raise ValueError("Already listening!")

        _LOGGER.debug("Executing listen hooks %s", self._listen_hooks)
        self.exec_hooks(self._listen_hooks)

        self._worker = Thread(name='listener', target=self._listen)
        self._worker.start()

    def _listen(self) -> None:
        _LOGGER.info('listener thread starting')

        self._running = True

        # send start bundle
        self._s.bundler().add(self._son.start()).send()

        for row in self._datagen():

            if not self._running:
                # close was called
                break

            self._s.bundler().add(self._son.process(row)).send()

            # if logging is enabled, log the data
            if self._logs is not None:
                # TODO: handle out of memory error
                self._logs = self._logs.append(row)

            if self._feature_display:
                self._feature_display.feed(row)

        # send stop bundle
        self._s.bundler().add(self._son.stop()).send()

        # this is relevant when the for loop ends naturally
        self._running = False
        _LOGGER.info('listener thread exiting')

    def add_listen_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'RTDataPlayer':
        self._listen_hooks.append((hook, args, kwargs))

        # return self for chaining
        return self

    def close(self) -> None:
        if not self._running:
            raise ValueError('Already closed!')
        # stop workin thread
        self._running = False
        self._worker.join()

        _LOGGER.debug("Executing close hooks %s", self._close_hooks)
        self.exec_hooks(self._close_hooks)

    def add_close_hook(self, hook: Callable[..., None], *args, **kwargs) -> 'RTDataPlayer':
        self._close_hooks.append((hook, args, kwargs))

        # return self for chaining
        return self

    @staticmethod
    def exec_hooks(hooks: List[Tuple[Callable[..., None], Any, Any]]):
        for hook, args, kwargs in hooks:
            if args and kwargs:
                hook(*args, **kwargs)
            elif args:
                hook(*args)
            elif kwargs:
                hook(**kwargs)
            else:
                hook()

    def record_start(self, path='record.wav') -> None:
        # TODO: this sends the recorder definition every time.
        # can we do better?

        if self._recorder is not None:
            raise ValueError("Recorder already working.")

        self._recorder = scn.Recorder(path=path, server=self._s)
        # send start bundle to the server
        self._recorder.start()

    def record_stop(self) -> None:
        if self._recorder is None:
            raise ValueError("Start the recorder first!")

        # send stop bundle to the server
        self._recorder.stop()
        self._recorder = None

    def log_start(self) -> None:
        if self._logs is not None:
            raise ValueError("Already logging.")
        self._logs = pd.DataFrame()

    def log_stop(self, path=None) -> pd.DataFrame:
        if self._logs is None:
            raise ValueError("Start logging first!")
        df = self._logs
        self._logs = None

        if path is not None:
            df.to_csv(path)

        return df

    def _get_ipywidget(self):
        listen = widgets.Button(icon='play')
        close = widgets.Button(icon='stop')
        # TODO: add pause button ("mute" is more appropriate)

        controls = widgets.HBox([listen, close])

        record = widgets.ToggleButton(
            value=False,
            description='Record',
            icon='microphone'
        )
        record_out = widgets.Text(
            value='record.wav',
            description='Output path:',
        )
        # TODO: overwrite check button
        record_box = widgets.HBox([record, record_out])

        log = widgets.ToggleButton(
            value=False,
            description='Log',
            icon='save'
        )
        log_out = widgets.Text(
            value='log.csv',
            description='Output path:',
        )
        # TODO: overwrite check button
        log_box = widgets.HBox([log, log_out])

        clear_out = widgets.Button(
            description='Clear output'
        )

        out = widgets.Output(layout={'border': '1px solid black'})

        def on_listen(button):
            with out:
                self.listen()

        def on_close(button):
            with out:
                self.close()

        listen.on_click(on_listen)
        close.on_click(on_close)

        def toggle_record(value):
            with out:
                if value['new']:
                    self.record_start(record_out.value)
                else:
                    self.record_stop()

        record.observe(toggle_record, 'value')

        def toggle_log(value):
            with out:
                if value['new']:
                    self.log_start()
                else:
                    self.log_stop(log_out.value)

        log.observe(toggle_log, 'value')

        def on_clear(button):
            out.clear_output()

        clear_out.on_click(on_clear)

        widget = widgets.VBox([controls, record_box, log_box, clear_out, out])

        return widget

    def _ipython_display_(self):
        display(self._widget)

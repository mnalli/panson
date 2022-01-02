import sc3nb as scn
from sc3nb import Score, Bundler

import pandas as pd

from time import time, sleep


class DataPlayer:

    def __init__(self, sonification=None, data=None):
        self.sonification = sonification
        if data is None:
            self.df = None
        else:
            self.load(data)

        # reference to default server
        self._s = scn.SC.get_default().server

    def load(self, data):
        if type(data) == type(str):
            self.df = self._load()
        elif type(data) == pd.DataFrame:
            self.df = data
        else:
            raise ValueError(
                f"Cannot load {data} of type {type(data)}."
                f"Needing {pd.DataFrame} or a path to a csv file."
            )

    @staticmethod
    def _load(df_path):
        # TODO: support all format automatically
        df = pd.read_csv(df_path, sep=r',\s*', engine='python')
        return df

    def play_test(self):
        # load synthdefs on the server
        # TODO: do it every time???
        self._s.bundler().add(self.sonification.initialize()).send()

        # TODO: is it better to instantiate asap?
        self._s.bundler().add(self.sonification.start()).send()
        # Bundler().add(son.start(sc.server)).send()

        t0 = time()

        # iterate over dataframe rows
        for _, row in self.df.iterrows():
            # process, bundle and send
            self._s.bundler(t0 + row.timestamp).add(self.sonification.process(row)).send()

            # sleep for the missing time
            waiting_time = t0 + row.timestamp - time()
            if waiting_time > 0:
                sleep(waiting_time)

        # send stop bundle
        self._s.bundler().add(self.sonification.stop()).send()

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
            for _, row in self.df.iterrows():
                # lend over row (pd.Series) to Sonification
                bundler.add(row.timestamp, self.sonification.process(row))

            # TODO: call sonification.stop???

            # TODO: this way the last line will not count? do we want to add an offset?
            # /c_set [0, 0] will close the audio file
            bundler.add(row.timestamp, "/c_set", [0, 0])

        return bundler.messages()

    def export(self, out_path):
        score = self._get_score()

        # TODO: support other headers and scorefile paths?
        Score.record_nrt(score, "/tmp/score.osc", out_path, header_format="WAV")

    # def __repr__(self):
    #     pass


# TODO: use callback instead of ABS with template method?
class RTDataPlayer():

    def __init__(self, sonification=None, data_generator=None):
        self.sonification = sonification
        self._datagen = data_generator
        self._s = scn.SC.get_default().server

    def listen(self):
        # load synthdefs on the server
        # TODO: do it every time???
        self._s.bundler().add(self.sonification.initialize()).send()

        # TODO: is it better to instantiate asap?
        self._s.bundler().add(self.sonification.start()).send()

        # the first line written to the pipe is the CSV header
        header = next(self._datagen)
        # print(header)

        for row in self._datagen:
            # TODO: check copying behaviour
            series = pd.Series(row, header, dtype='float')
            self._s.bundler().add(self.sonification.process(series)).send()

        # send stop bundle
        self._s.bundler().add(self.sonification.stop()).send()

    def record_start(self, out):
        pass

    def record_stop(self):
        pass

    def log_start(self, out):
        pass

    def log_stop(self, return_dp=False):
        pass
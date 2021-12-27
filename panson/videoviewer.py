import time
import numpy as np

import threading

from PyQt5 import QtWidgets, QtCore

import pyqtgraph as pg
import skvideo
import skvideo.io

from sys import argv

skvideo.setFFmpegPath('/usr/bin')


class VideoViewer(threading.Thread):

    def __init__(self, rect=(1300, 0, 620, 400), decim=2):
        super().__init__()

        # the program exits when no alive non-daemon threads are left
        self.daemon = True
        # pass lock explicitly to use the non-reentrant lock
        self._run_cond = threading.Condition()
        # TODO: this variable is not thread safe!!!
        self._running = None
        self._current_frame_idx = None

        self._x0, self._y0, self._width, self._height = rect
        self._filename = None
        self._decim = decim
        self._channels = [0, 1, 2]
        self._frames = None
        self._nrframes = None
        self._video_height = 0
        self._video_width = 0
        self._video_nr_channels = None
        self._duration = None
        self._fps = None
        self.frametimes = None

        self.app = QtWidgets.QApplication([])

        # GUI initialization
        self._win = QtWidgets.QWidget()

        self._pause_button = QtWidgets.QPushButton('pause')
        self._pause_button.clicked.connect(self.pause)

        self._play_button = QtWidgets.QPushButton('play')
        self._play_button.clicked.connect(self.resume)

        self._imggv = pg.GraphicsView()
        self._viewbox = pg.ViewBox()
        self._imggv.setCentralItem(self._viewbox)
        self._viewbox.setAspectLocked()
        self._viewbox.invertY(True)

        self._img = pg.ImageItem(np.zeros((100, 100, 3)))  # Todo: 3 -> channel variable
        self._viewbox.addItem(self._img)

        layout = QtWidgets.QGridLayout()
        self._win.setLayout(layout)
        layout.addWidget(self._imggv, 0, 0, 4, 4)
        layout.addWidget(self._pause_button, 5, 0)
        layout.addWidget(self._play_button, 5, 1)

        self._win.show()
        self._win.resize(self._width, self._height)
        self._win.move(self._x0, self._y0)

    def run(self):

        # this is a daemon: it will run as long as the viewer run
        while True:
            with self._run_cond:
                self.seek(self._current_frame_idx)
                self._current_frame_idx += 1

                # if the last frame was just displayed
                if self._current_frame_idx == self._nrframes:
                    # set current frame to last frame, so that, if the video is resumed, the thread will block again
                    self._current_frame_idx -= 1
                    self._run_cond.wait()

            # TODO: use correct frame times for this playback....
            time.sleep(0.03)
            # it is not correct to keep it here

    def load_video(self, filename):
        """
        Load video from file.

        Loads video file filename into memory and checks if a file filename.csv exists,
        if so this is loaded and later used for seek_time(). filename.csv must contain
        a comma-separated list of frame timestamps.

        Parameters
        ----------
        filename: video filename with suffix, e.g. .avi

        Returns
        -------

        """
        self._filename = filename

        # load all video in memory
        vdict = skvideo.io.ffprobe(self._filename)['video']
        self._duration = float(vdict['@duration'])
        self._nrframes = int(vdict['@duration_ts'])
        self._fps = self._nrframes / self._duration

        self._frames = skvideo.io.vread(self._filename)
        self._nrframes, self._video_height, self._video_width, self._video_nr_channels = np.shape(self._frames)

        self._current_frame_idx = 0
        # display initial frame
        self.seek(self._current_frame_idx)
        # pause the thread on start
        self._running = True
        self.pause()

        try:
            # load csv list of frame timestamps
            self.frametimes = np.loadtxt(self._filename + ".csv", delimiter=",")
        except FileNotFoundError:
            # print("file not found", file=sys.stderr)
            self.frametimes = None

        return self._nrframes, self._fps

    def seek(self, frame_idx):
        """
        Display frame on the video viewer.

        :param frame_idx: index of the frame to display
        :return:
        """

        assert not self._nrframes is None, "No video loaded in the viewer"

        assert 0 <= frame_idx < self._nrframes,\
            ("frame_idx is %d. Should be between 0 and %d" % frame_idx, self._nrframes)

        frame = self._frames[frame_idx, ::self._decim, ::self._decim, self._channels].transpose()

        # (144, 256, 3)
        # print(np.shape(frame))

        self._img.setImage(frame)
        # self.app.processEvents() # ToDo: understand why this creates such a headache if called from run (thread)

    def seek_time(self, timestamp):
        """
        Display frame at the specified timestamp on the video viewer.

        :param timestamp:
        :return: index of the target frame
        """

        assert timestamp >= 0, "timestamp is %x, must be >= 0"

        if self.frametimes is None:
            frame_idx = int(timestamp * self._fps)
        else:
            # TODO: accelerate forward searching in monotonous functions
            frame_idx = np.argmin(np.abs(self.frametimes - timestamp))

        self.seek(frame_idx)

        return frame_idx

    def pause(self):
        if self._running:
            self._running = False
            # acquire condition to pause the thread
            self._run_cond.acquire()

    def resume(self):
        if not self._running:
            self._running = True
            # wake up thread
            self._run_cond.notify()
            # release the lock
            self._run_cond.release()


class NBVideoViewer(threading.Thread):

    def __init__(self, rect=(1300, 0, 620, 400), decim=2):
        super().__init__()

        self._x0, self._y0, self._width, self._height = rect
        self._decim = decim
        self._channels = [0, 1, 2]
        self._nrframes = None
        self._video_height = 0
        self._video_width = 0
        self._video_nr_channels = None

        # GUI initialization
        self._win = QtWidgets.QWidget()

        self._imggv = pg.GraphicsView()
        self._viewbox = pg.ViewBox()
        self._imggv.setCentralItem(self._viewbox)
        self._viewbox.setAspectLocked()
        self._viewbox.invertY(True)

        self._img = pg.ImageItem(np.zeros((100, 100, 3)))  # Todo: 3 -> channel variable
        self._viewbox.addItem(self._img)

        layout = QtWidgets.QGridLayout()
        self._win.setLayout(layout)
        layout.addWidget(self._imggv, 0, 0, 4, 4)

        # window always on top
        self._win.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint)

        self._win.show()
        # self._win.resize(self._width, self._height)
        # self._win.move(self._x0, self._y0)

        self.daemon = True
        self._update_cond = threading.Condition()
        self._frame = None

    def update(self, frame):
        with self._update_cond:
            # fetch frame
            self._frame = frame
            # wake up displaying thread
            self._update_cond.notify()

    def run(self):
        while True:
            with self._update_cond:
                if not self._frame is None:
                    self._img.setImage(self._frame.T)
                self._update_cond.wait()




if __name__ == "__main__":
    v = VideoViewer(decim=5)
    v.load_video(argv[1])
    v.start()  # starts the thread as run()
    exit(v.app.exec_())

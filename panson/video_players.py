
import time

from PyQt5 import QtWidgets, QtCore

from PyQt5.QtCore import (
    QCoreApplication, QObject, QRunnable, QThread, QThreadPool, pyqtSignal
)

import pyqtgraph as pg

import cv2

import threading
import sys

import multiprocessing as mp
import numpy as np

import pims


class Communicate(QObject):

    updateImg = pyqtSignal(np.ndarray)


class VideoPlayerServer:

    def __init__(
        self,
        file_path: str,
        conn: mp.connection.Connection,
        on_top: bool = True
    ):
        self._file_path = file_path
        # TODO: Video is the correct class to use?
        self._video = pims.Video(file_path)

        # pipe end
        self._conn = conn

        # threads
        self._receiver_thread = threading.Thread(target=self._receiver)

        self.c = Communicate()
        self.c.updateImg.connect(self.update_img)

        # playback running
        self._running = False

        # GUI
        self.app = QtWidgets.QApplication([])
        self._win = QtWidgets.QMainWindow()
        self._win.setWindowTitle(f'File: {file_path}')

        # window always on top
        if on_top:
            self._win.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint)

        # black image
        self._img = pg.ImageItem()
        self._img.setAutoDownsample(True)

        self._img.setImage(self._video[0].swapaxes(0, 1))

        self._img_gv = pg.GraphicsView()
        self._view_box = pg.ViewBox()
        self._view_box.setAspectLocked()
        self._view_box.invertY(True)

        self._img_gv.setCentralItem(self._view_box)

        self._view_box.addItem(self._img)

        self._win.setCentralWidget(self._img_gv)

        # print(f"{self._width} x {self._height} @ {self._fps} fps")
        # self._win.setGeometry(1000, 0, self._width, self._height)

        self._win.show()

    def update_img(self, img):
        self._img.setImage(img)

    def start(self):
        self._running = True
        self._receiver_thread.start()

    def _receiver(self):
        while self._running:
            cmd = self._conn.recv()
            self.__getattribute__(cmd[0])(*cmd[1:])

        # end main loop
        self.app.exit()

    # COMMANDS

    def seek(self, idx: int):
        frame = self._video[idx]
        self.c.updateImg.emit(frame.swapaxes(0, 1))

    def quit(self):
        self._running = False
        self._conn.send(0)
        self._conn.close()


class VideoPlayer:

    def __init__(self, file_path: str, **kwargs):

        self._conn, child_conn = mp.Pipe()
        p = mp.Process(
            target=self._server_main,
            args=(file_path, child_conn,),
            kwargs=kwargs,
        )

        # start server process
        p.start()

    @staticmethod
    def _server_main(file_path, conn, **kwargs):
        vp = VideoPlayerServer(file_path, conn, **kwargs)
        # start threads
        vp.start()
        # start main loop
        sys.exit(vp.app.exec())

    def get_reply(self):
        return self._conn.recv()

    def seek(self, idx: int):
        self._conn.send(('seek', idx))

    def quit(self):
        self._conn.send(('quit',))
        self._conn.close()


class RTVideoPlayerServer:

    def __init__(
        self,
        conn: mp.connection.Connection,
        device: int = 0,
        auto_enum_files: bool = True,
        on_top: bool = True
    ):
        # pipe end
        self._conn = conn

        # threads
        self._receiver_thread = threading.Thread(target=self._receiver)
        self._recorder_thread = threading.Thread(target=self._recorder)

        self.c = Communicate()
        self.c.updateImg.connect(self.update_img)

        # playback running
        self._running = False
        self._recording = False

        # file name
        self._out_file_prefix = 'test'
        self._out_file_suffix = 'avi'

        #
        self._device_id = device

        #
        self._t0 = None

        # count number of times record is called
        self._run_counter = 0
        self.auto_enumerate_files = auto_enum_files

        # GUI
        self.app = QtWidgets.QApplication([])
        self._win = QtWidgets.QMainWindow()
        self._win.setWindowTitle(f'Camera: device {device}')

        # black image
        self._img = pg.ImageItem()
        self._img.setAutoDownsample(True)

        self._img_gv = pg.GraphicsView()
        self._view_box = pg.ViewBox()
        self._view_box.setAspectLocked()
        self._view_box.invertY(True)

        self._img_gv.setCentralItem(self._view_box)

        self._view_box.addItem(self._img)

        self._win.setCentralWidget(self._img_gv)

        # window always on top
        if on_top:
            self._win.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint)

        self._fourcc = cv2.VideoWriter_fourcc(*'XVID')
        self._writer: cv2.VideoWriter = None

        # capture device
        self._capture = cv2.VideoCapture(self._device_id)
        # get actual parameters
        self._width = int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        self._height = int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self._fps = self._capture.get(cv2.CAP_PROP_FPS)

        print(f"{self._width} x {self._height} @ {self._fps} fps")

        self._win.setGeometry(1000, 0, self._width, self._height)
        self._win.show()

    def update_img(self, img):
        self._img.setImage(img)

    def start(self):
        self._running = True
        self._receiver_thread.start()
        self._recorder_thread.start()

    def start_recording(self):
        if self.auto_enumerate_files:
            self.fname = "%s-%03d.%s" % (self._out_file_prefix, self._run_counter, self._out_file_suffix)
            self._run_counter += 1
        else:
            self.fname = self._out_file_prefix + "." + self._out_file_suffix

        self._writer = cv2.VideoWriter(
            self.fname, self._fourcc, self._fps, (self._width, self._height)
        )

        self.frame_counter = 0
        # to hold [cnt, timestamp]
        self.frametimes = []
        self._t0 = time.time()

    def stop_recording(self):
        self._writer.release()
        print("Release writer")

        np.savetxt(
            f"{self.fname}.csv",
            np.array(self.frametimes),
            delimiter=',',
            fmt='%g',
            header="# frame_number, timestamp[s]"
        )

    def _receiver(self):
        while self._running:
            cmd = self._conn.recv()
            self.__getattribute__(cmd[0])(*cmd[1:])

    def _recorder(self):

        while self._running:
            grabbed, frame = self._capture.read()

            assert grabbed

            self.c.updateImg.emit(frame[:, :, (2, 1, 0)].swapaxes(0, 1))

            if self._recording:
                t = time.time()

                self.frametimes.append((self.frame_counter, t - self._t0))
                self.frame_counter += 1

                self._writer.write(frame)

        if self._recording:
            self.stop_recording()

        self._capture.release()
        print("Release capture")

        # end main loop
        self.app.exit()

    # COMMANDS

    def filename(self, name: str):
        """Set capture file name.

        Set also suffix (and thus video file containter format).
        """
        self._out_file_prefix, self._out_file_suffix = name.split('.')
        self._conn.send(0)

    def autoenum(self, amount: bool):
        """Enable or disable auto enumeration of files."""
        self.auto_enumerate_files = amount
        self._conn.send(0)

    def record(self):
        self.start_recording()
        self._recording = True
        self._conn.send(self._t0)

    def stop(self):
        self.stop_recording()
        # cancel recording - will let thread run come to an end
        self._recording = False
        self._conn.send(0)

    def quit(self):
        self._running = False
        self._conn.send(0)
        self._conn.close()


class RTVideoPlayer:

    def __init__(self, **kwargs):

        self._conn, child_conn = mp.Pipe()
        p = mp.Process(
            target=self._server_main,
            args=(child_conn,),
            kwargs=kwargs,
        )

        # start server process
        p.start()

    @staticmethod
    def _server_main(conn, **kwargs):
        vp = RTVideoPlayerServer(conn, **kwargs)
        # start threads
        vp.start()
        # start main loop
        sys.exit(vp.app.exec())

    def get_reply(self):
        return self._conn.recv()

    def set_auto_enum_files(self, val: bool):
        self._conn.send(('autoenum', {val}))

    def set_filename(self, filename: str):
        self._conn.send(('filename', filename))

    def record(self):
        self._conn.send(('record',))

    def stop(self):
        self._conn.send(('stop',))

    def quit(self):
        self._conn.send(('quit',))
        self._conn.close()

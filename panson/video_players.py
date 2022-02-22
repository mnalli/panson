
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

import traceback


class Communicate(QObject):

    updateImg = pyqtSignal(np.ndarray)


class VideoPlayerServer:

    def __init__(
            self,
            conn: mp.connection.Connection,
            file_path: str,
            frame_times_path: str = None,
            fps=None,
            on_top: bool = True
    ):
        # pipe end
        self._conn = conn

        self._file_path = file_path
        # TODO: Video is the correct class to use?
        self._video = pims.Video(file_path)

        if frame_times_path is None:
            print('No frame times specified')

            if fps is None:
                print('No FPS specified')
                self._fps = None

                frame_times_path = file_path + '.csv'
                print(f'Loading default frame times from {frame_times_path}')

                try:
                    self._frame_times = np.loadtxt(frame_times_path, delimiter=',')
                    print('Frame times loaded.')
                except OSError:
                    print('No default frame times found: seek_time will not work.')
                    self._frame_times = None

            else:
                print(f'Using static fps {fps}')
                self._fps = fps
                self._frame_times = None
        else:
            print(f'Loading frame times from {frame_times_path}')
            self._frame_times = np.loadtxt(frame_times_path)
            if fps:
                print(f'Ignoring specified fps {fps}')
                self._fps = None

        # threads
        self._receiver_thread = threading.Thread(target=self._receiver)

        self.c = Communicate()
        self.c.updateImg.connect(self._update_img)

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

        self._img_gv = pg.GraphicsView()
        self._view_box = pg.ViewBox()
        self._view_box.setAspectLocked()
        self._view_box.invertY(True)

        self._img_gv.setCentralItem(self._view_box)

        self._view_box.addItem(self._img)

        self._win.setCentralWidget(self._img_gv)

        # print(f"{self._width} x {self._height} @ {self._fps} fps")
        # self._win.setGeometry(1000, 0, self._width, self._height)

        # set first frame
        self._curr_frame_idx = 0
        self._img.setImage(self._video[self._curr_frame_idx].swapaxes(0, 1))

        self._win.show()

    def _update_img(self, img):
        self._img.setImage(img)

    def start(self):
        self._running = True
        self._receiver_thread.start()

    def _receiver(self):
        while self._running:
            cmd = self._conn.recv()

            try:
                self.__getattribute__(cmd[0])(*cmd[1:])
            except:
                # capture and print all exception not to make the server crash
                traceback.print_exc()

        # end main loop
        self.app.exit()

    # COMMANDS

    def seek(self, idx: int):
        if idx == self._curr_frame_idx:
            return

        # TODO: check boundaries

        frame = self._video[idx]
        self.c.updateImg.emit(frame.swapaxes(0, 1))

        self._curr_frame_idx = idx

        self._conn.send(0)

    def seek_time(self, t: float):
        if self._frame_times is not None:
            max_time = self._frame_times[-1, 1]
            if t > max_time:
                raise ValueError(
                    f"t == {t} is greater than maximum time {max_time}"
                )
            # find timestamp with binary search
            idx = np.searchsorted(self._frame_times[:, 1], t)
        elif self._fps is not None:
            max_time = len(self._video) / self._fps
            if t > max_time:
                raise ValueError(
                    f"t == {t} is greater than maximum time {max_time}"
                )
            idx = int(t * self._fps)
        else:
            raise ValueError(
                "Cannot use seek_time when frame times and fps are not specified."
            )

        self.seek(idx)

    def quit(self):
        self._running = False
        self._conn.send(0)
        self._conn.close()


class VideoPlayer:

    def __init__(
            self,
            file_path: str,
            frame_times_path: str = None,
            fps=None,
            on_top: bool = True
    ):

        self._conn, child_conn = mp.Pipe()
        p = mp.Process(
            target=self._server_main,
            args=(child_conn, file_path, frame_times_path, fps, on_top),
        )

        # start server process
        p.start()

    @staticmethod
    def _server_main(*args):
        vp = VideoPlayerServer(*args)
        # start threads
        vp.start()
        # start main loop
        sys.exit(vp.app.exec())

    def seek(self, idx: int):
        self._conn.send(('seek', idx))

    def seek_time(self, t: float):
        self._conn.send(('seek_time', t))

    def get_reply(self):
        return self._conn.recv()

    def quit(self):
        self._conn.send(('quit',))
        self._conn.close()

    def __del__(self):
        if not self._conn.closed:
            self.quit()


class RTVideoPlayerServer:

    def __init__(
            self,
            conn: mp.connection.Connection,
            device: int = 0,
            width: int = None,
            height: int = None,
            fps: int = None,
            enumerate_records: bool = True,
            on_top: bool = True
    ):
        # pipe end
        self._conn = conn

        # threads
        self._receiver_thread = threading.Thread(target=self._receiver)
        self._recorder_thread = threading.Thread(target=self._recorder)

        self.c = Communicate()
        self.c.updateImg.connect(self._update_img)

        # playback running
        self._running = False

        # file name
        self._out_file_prefix = 'record'
        self._out_file_suffix = 'avi'

        self._device_id = device

        # Recorder
        self._recording = False
        # recorder's start time
        self._t0 = None
        self._enumerate_records = enumerate_records
        self._run_counter = 0
        self._frame_counter = None
        self._frametimes = None
        self._fname = None

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

        # try to set parameters specified by the user
        if width:
            self._capture.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        if height:
            self._capture.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        if fps:
            self._capture.set(cv2.CAP_PROP_FPS, fps)

        # get actual parameters
        self._width = int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        self._height = int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self._fps = self._capture.get(cv2.CAP_PROP_FPS)

        print(f"{self._width} x {self._height} @ {self._fps} fps")

        # TODO: relative window position
        self._win.setGeometry(1000, 0, self._width, self._height)
        self._win.show()

    def _update_img(self, img):
        self._img.setImage(img)

    def start(self):
        self._running = True
        self._receiver_thread.start()
        self._recorder_thread.start()

    def start_recording(self):
        if self._enumerate_records:
            self._fname = "%s-%03d.%s" % (self._out_file_prefix, self._run_counter, self._out_file_suffix)
            self._run_counter += 1
        else:
            self._fname = self._out_file_prefix + "." + self._out_file_suffix

        self._writer = cv2.VideoWriter(
            self._fname, self._fourcc, self._fps, (self._width, self._height)
        )

        self._frame_counter = 0
        # to hold [counter_val, timestamp]
        self._frametimes = []
        self._t0 = time.time()

    def stop_recording(self):
        self._writer.release()
        print("Release writer")

        np.savetxt(
            f"{self._fname}.csv",
            np.array(self._frametimes),
            delimiter=',',
            fmt='%g',
            header="frame_number, timestamp"
        )

    def _receiver(self):
        while self._running:
            cmd = self._conn.recv()

            try:
                self.__getattribute__(cmd[0])(*cmd[1:])
            except:
                # capture and print all exception not to make the server crash
                traceback.print_exc()

    def _recorder(self):

        while self._running:
            grabbed, frame = self._capture.read()
            t = time.time()

            if not grabbed:
                print("Can't receive frame (stream end?). Exiting ...")
                self._running = False
                break

            self.c.updateImg.emit(frame[:, :, (2, 1, 0)].swapaxes(0, 1))

            if self._recording:
                self._frametimes.append((self._frame_counter, t - self._t0))
                self._frame_counter += 1

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

    def autoenum(self, val: bool):
        """Enable or disable auto enumeration of files."""
        self._enumerate_records = val
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

    def __init__(
            self,
            device: int = 0,
            width: int = None,
            height: int = None,
            fps: int = None,
            enumerate_records: bool = True,
            on_top: bool = True
    ):

        self._conn, child_conn = mp.Pipe()
        p = mp.Process(
            target=self._server_main,
            args=(child_conn, device, width, height, fps, enumerate_records, on_top),
        )

        # start server process
        p.start()

    @staticmethod
    def _server_main(*args):
        vp = RTVideoPlayerServer(*args)
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

    def __del__(self):
        if not self._conn.closed:
            self.quit()

#!/Applications/anaconda/bin/python

import time
import numpy as np
import cv2
import threading
from PyQt5 import QtWidgets
import pyqtgraph as pg
import subprocess
import os


class VCR(threading.Thread):

    def __init__(self, device=0, filename="out.avi", rect=(400, 400, 1000, 0), video_width=1280,
                 video_height=720, fps=30, decim=2, channels=(2, 1, 0), auto_enum_files=True):
        threading.Thread.__init__(self)
        self._width, self._height, self._x0, self._y0 = rect
        self._cancel = 0
        self._recording = 0
        self._capture = None
        self._out_fname_prefix, self._out_fname_suffix = filename.split('.')
        self._device_id = device
        self._fps = fps
        self._decim = decim
        self._channels = channels
        self._width = video_width
        self._height = video_height
        self._t0 = time.time()
        self._writer = None
        self._fourcc = None
        self._frametimes = []  # to hold [cnt, timestamp]
        self._run_counter = 0
        self._btn_prepare = None
        self.auto_enumerate_files = auto_enum_files

    def init(self):
        self._app = QtWidgets.QApplication([])
        self._win = QtWidgets.QWidget()

        self._btn_prepare = QtWidgets.QPushButton('prepare')
        self._btn_prepare.clicked.connect(self.recorder)

        self._imggv = pg.GraphicsView()
        self._img = pg.ImageItem(np.zeros((640, 360, 3)))  # Todo: 3 -> channel variable
        self._imggv.addItem(self._img)

        layout = QtWidgets.QGridLayout()
        self._win.setLayout(layout)
        layout.addWidget(self._imggv, 0, 0)
        layout.addWidget(self._btn_prepare, 1, 0)
        self._win.show()
        self._win.resize(640, 480)
        self._win.move(1000, 0)
        self._capture = cv2.VideoCapture(self._device_id)
        self._capture.set(cv2.CAP_PROP_FRAME_WIDTH, self._width)  # 1280
        self._capture.set(cv2.CAP_PROP_FRAME_HEIGHT, self._height)  # 720
        self._capture.set(cv2.CAP_PROP_FPS, self._fps)
        self._fourcc = cv2.VideoWriter_fourcc(*'XVID')
        self._width = int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        self._height = int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self._fps = self._capture.get(cv2.CAP_PROP_FPS)

    def run(self):
        def confirm(val):
            print(val)

        _is_running = 1

        while _is_running:
            try:
                rl = input().split(' ')
            except EOFError:
                _is_running = 0

            if rl[0] == 'filename':  # set capture file name: sets also suffix and thus video file containter format
                self._out_fname_prefix, self._out_fname_suffix = rl[1].split('.')
                confirm(0)

            if rl[0] == 'decim':  # set decimator for real-time display in Qt viewer
                self._decim = int(rl[1])
                confirm(0)

            if rl[0] == 'autoenum':  # 0 or 1
                self.auto_enumerate_files = int(rl[1])
                confirm(0)

            if rl[0] == 'prepare':  # starts recorder routine which creates the cv2.writer
                self._cancel = 0  # so that recorder stays in loop
                self._btn_prepare.click()
                confirm(0)

            if rl[0] == 'record':
                self._recording = 1
                self._t0 = time.time()
                confirm(self._t0)

            if rl[0] == 'pause':
                self._recording = 0
                confirm(0)

            if rl[0] == 'stop':  # cancel recording - will let thread run come to an end
                self._cancel = 1
                confirm(0)

            if rl[0] == 'quit':  # terminate viewer
                _is_running = 0
                confirm(0)
        print("end of thread")
        self._capture.release()
        print("VCR: Release capture")
        os._exit(0)

    def exit(self):
        self._cancel = 1
        time.sleep(0.2)  # ToDo: check if needed and if so how to do differently
        self._capture.release()
        print("VCR: Release capture")
        # self.join()
        self._app.exit()

    def recorder(self):
        ct = 0
        if self.auto_enumerate_files:
            self._run_counter += 1
            fname = "%s-%03d.%s" % (self._out_fname_prefix, self._run_counter, self._out_fname_suffix)
        else:
            fname = self._out_fname_prefix + "." + self._out_fname_suffix

        self._writer = cv2.VideoWriter(fname, self._fourcc, self._fps,
                                       (self._width, self._height))
        self._frametimes = []
        self._t0 = time.time()
        while not self._cancel:
            if self._recording:
                ret, frame = self._capture.read()
                t = time.time()
                # print(t - self._t0)
                self._frametimes.append([ct, t - self._t0])
                ct += 1
                if ret:
                    self._writer.write(frame)
                    self._img.setImage(frame[::self._decim, ::self._decim, self._channels].swapaxes(0, 1))
                    pg.QtGui.QApplication.processEvents()
                else:
                    pass
            else:
                time.sleep(1 / self._fps)
                pg.QtGui.QApplication.processEvents()

        self._writer.release()
        print("VCR: release writer")
        fname_csv = "%s.csv" % (fname,)
        np.savetxt(fname_csv, np.array(self._frametimes), delimiter=',', fmt='%g',
                   header="# frame_number, timestamp[s]")
        self._cancel = 0


class VCRManager:

    def __init__(self, auto_enum_files=1):
        self.vcr_process = subprocess.Popen(["./vcr_gui_pipe.py"], shell=False, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, universal_newlines=True)
        self.set_auto_enum_files(auto_enum_files)

    def cmd(self, cmd_str):
        """"send command string cmd_str to process stdin to be parsed and executed.
        cmd blocks and returns stdout pipe reply.
        """
        self.vcr_process.stdin.write(cmd_str + "\n")
        self.vcr_process.stdin.flush()
        return self.vcr_process.stdout.readline()

    def get_reply(self):
        return self.vcr_process.stdout.readline()

    def set_auto_enum_files(self, val):
        return self.cmd("autoenum %s" % (val,))

    def set_filename(self, filename):
        return self.cmd("filename %s" % (filename,))

    def set_decimate(self, decimate):
        return self.cmd("decim %d", (decimate,))

    def prepare(self):
        return self.cmd("prepare")

    def record(self):
        return self.cmd("record")

    def pause(self):
        return self.cmd("pause")

    def cancel(self):
        return self.cmd("stop")

    def quit(self):
        self.cmd("quit")
        # self.vcr_process.stdin.close()
        return 0  #

if __name__ == "__main__":
    v = VCR(device=1)
    v.init()
    v.start()  # starts the thread as run
    v._app.exec_()

    # uncomment following lines for VCRManager class test

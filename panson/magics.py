"""This module adds the Jupyter Magics"""

# TODO: this does not really belong to the panson project...
# should we move it out in a separate and separately installable project?

import tempfile
import os
import subprocess

from IPython.core.magic import Magics, magics_class, line_magic, cell_magic

# TODO: create directory here?
_TMP_DIR = tempfile.TemporaryDirectory(prefix='jupyter_editor_')

# TOOD: check %load magic
# TODO: for now we have to load magics with %load_exp panson. How to do it automatically?
# TODO: check %edit arguments
# TODO: allow to put file in the same directory as the notebook, so that language
#   tools work well


@magics_class
class PansonMagics(Magics):

    @cell_magic
    def editor(self, line="", cell=None):
        """Edit cell using the default text editor.

        This is similar to the %edit ipython magic, but adapted to work in a
        jupyter notebook.
        """

        # print(line, cell)

        tmp_file = tempfile.NamedTemporaryFile(prefix='jupyter_editor_', suffix='.py', dir=_TMP_DIR.name, delete=False)
        print("Jupyter will make a temporary file named:", tmp_file.name)

        # write current cell to the file
        with open(tmp_file.name, 'w') as f:
            f.write(cell)

        # open editor
        # TODO: if not defined?
        editor_cmd = os.environ.get('VISUAL').split()
        editor_cmd.append(f.name)
        subprocess.run(editor_cmd)

        # write current cell to the file
        with open(tmp_file.name, 'r') as f:
            code = f.read()

        # add a cell after the current one with the code
        self.shell.set_next_input(code, replace=False)
        # Uncomment this line if you want to run the code instead.
        # self.shell.run_cell(code, store_history=False)

        # return "opening editor", line


def load_ipython_extension(ipython) -> None:
    """Function that is called when Jupyter loads this as extension (%load_ext sc3nb)

    Parameters
    ----------
    ipython : IPython
        IPython object
    """

    ipython.register_magics(PansonMagics)

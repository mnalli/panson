import ipywidgets as widgets
from ipywidgets import HBox, VBox
from IPython.display import display


class RTDataPlayerWidgetView:

    def __init__(self, player):
        self._player = player

        # player controls
        listen = widgets.Button(icon='play')
        close = widgets.Button(icon='stop')
        # TODO: add pause button ("mute" is more appropriate)

        # recorder controls
        record = widgets.ToggleButton(
            value=False,
            description='Record',
            icon='microphone'
        )
        self._record_output = widgets.Text(
            value='record.wav',
            description='Output path:',
        )
        self._record_overwrite = widgets.Checkbox(
            value=False,
            description='Overwrite'
        )

        # data logger controls
        log = widgets.ToggleButton(
            value=False,
            description='Log',
            icon='save'
        )
        self._log_output = widgets.Text(
            value='log.csv',
            description='Output path:',
        )
        self._log_overwrite = widgets.Checkbox(
            value=False,
            description='Overwrite'
        )

        # clear output button
        clear_output = widgets.Button(
            description='Clear output'
        )

        self._out = widgets.Output(layout={'border': '1px solid black'})

        # bind callback methods
        listen.on_click(self._on_listen)
        close.on_click(self._on_close)

        record.observe(self._toggle_record, 'value')
        log.observe(self._toggle_log, 'value')

        clear_output.on_click(self._on_clear)

        self._widget = VBox([
            HBox([listen, close]),
            HBox([record, self._record_output, self._record_overwrite]),
            HBox([log, self._log_output, self._log_overwrite]),
            clear_output,
            self._out
        ])

    def _ipython_display_(self):
        display(self._widget)

    def print(self, s: str):
        """Print using output widget."""
        with self._out:
            print(s)

    def _on_listen(self, button):
        with self._out:
            self._player.listen()

    def _on_close(self, button):
        with self._out:
            self._player.close()

    def _toggle_record(self, value):
        with self._out:
            if value['new']:
                self._player.record_start(
                    self._record_output.value,
                    overwrite=self._record_overwrite.value
                )
            else:
                self._player.record_stop()

    def _toggle_log(self, value):
        with self._out:
            if value['new']:
                self._player.log_start(
                    self._log_output.value,
                    overwrite=self._log_overwrite.value
                )
            else:
                self._player.log_stop()

    def _on_clear(self, button):
        self._out.clear_output()


class RTDataPlayerMultiWidgetView(RTDataPlayerWidgetView):

    def __init__(self, player):
        super().__init__(player)

        self.stream_log_boxes = []

        for i in range(len(player._streams)):
            log = widgets.ToggleButton(
                value=False,
                description='Log',
                icon='save'
            )
            log_output = widgets.Text(
                value=f'{i}_log.csv',
                description='Output path:',
            )
            log_overwrite = widgets.Checkbox(
                value=False,
                description='Overwrite'
            )
            self.stream_log_boxes.append(HBox([log, log_output, log_overwrite]))

        for i, log_box in enumerate(self.stream_log_boxes):
            log_box.children[0].observe(self._toggle_log_stream_gen(i), 'value')

        self._widget = VBox([
            *self._widget.children[:3],
            *self.stream_log_boxes,
            *self._widget.children[3:]
        ])

    def _toggle_log_stream_gen(self, idx):
        def toggle_log_stream(value):
            with self._out:
                if value['new']:
                    self._player.log_start_stream(
                        idx,
                        self.stream_log_boxes[idx].children[1].value,
                        overwrite=self.stream_log_boxes[idx].children[2].value
                    )
                else:
                    self._player.log_stop_stream(idx)

        return toggle_log_stream
import ipywidgets as widgets
from ipywidgets import HBox, VBox
from IPython.display import display


class DataPlayerWidgetView:

    def __init__(self, player, max_idx):
        self._player = player

        self._slider = widgets.IntSlider(
            value=self._player.ptr,
            min=0,
            max=max_idx,
            layout=widgets.Layout(width='98%'),
            # continuous_update=False
        )

        beginning = widgets.Button(icon='fast-backward')
        end = widgets.Button(icon='fast-forward')

        backward = widgets.Button(icon='step-backward')
        forward = widgets.Button(icon='step-forward')

        pause = widgets.Button(icon='pause')
        play = widgets.Button(icon='play')

        rate = widgets.FloatText(
            value=self._player.rate,
            description='Rate:',
        )

        self.record_button = widgets.ToggleButton(
            value=False,
            description='Record',
            icon='microphone'
        )
        self.record_output = widgets.Text(
            value='record.wav',
            description='Output path:',
        )
        self.record_overwrite = widgets.Checkbox(
            value=False,
            description='Overwrite'
        )

        self.export_button = widgets.Button(
            description='Export',
            icon='level-down'
        )
        self.export_output = widgets.Text(
            value='out',
            description='Output path:',
        )
        self.export_format = widgets.Dropdown(
            options=['WAV', 'AIFF'],
            value='WAV'
        )

        self._widget = VBox([
            self._slider,
            HBox([beginning, backward, pause, play, forward, end]),
            rate,
            HBox([self.record_button, self.record_output, self.record_overwrite]),
            HBox([self.export_button, self.export_output, self.export_format])
        ])

        # bind callbacks
        self._slider.observe(self._on_change, 'value')

        beginning.on_click(self._on_beginning)
        end.on_click(self._on_end)

        backward.on_click(self._on_backward)
        forward.on_click(self._on_forward)

        pause.on_click(self._on_pause)
        play.on_click(self._on_play)

        rate.observe(self._on_rate, 'value')

        self.record_button.observe(self._toggle_record, 'value')
        self.export_button.on_click(self._on_export)

    def _ipython_display_(self):
        display(self._widget)

    def update_slider(self, value):
        """Update slider without triggering any callback."""
        assert 0 <= value <= self._slider.max

        # unobserve callback
        self._slider.unobserve(self._on_change, 'value')
        # update
        self._slider.value = value
        # observe again
        self._slider.observe(self._on_change, 'value')

    def update_slider_max(self, value):
        """Change maximum value of the slider.

        Useful when loading new data.
        """
        self._slider.max = value

    def _on_change(self, value):
        # seek index
        self._player.seek(value['new'])

    def _on_beginning(self, button):
        self._slider.value = 0

    def _on_end(self, button):
        self._slider.value = self._slider.max

    def _on_pause(self, button):
        self._player.pause()

    def _on_play(self, button):
        self._player.play()

    # TODO: atomicity of the update?
    def _on_backward(self, button):
        self._slider.value -= 10

    def _on_forward(self, button):
        self._slider.value += 10

    def _on_rate(self, value):
        self._player.rate = value['new']

    def _toggle_record(self, value):
        if value['new']:
            self._player.record_start(
                self.record_output.value,
                overwrite=self.record_overwrite.value
            )
        else:
            self._player.record_stop()

    def _on_export(self, button):
        self._player.export(self.export_output.value, header_format=self.export_format.value)


class RTDataPlayerWidgetView:

    def __init__(self, player):
        self._player = player

        # player controls
        listen = widgets.Button(icon='play')
        close = widgets.Button(icon='stop')

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

        # bind callback methods
        listen.on_click(self._on_listen)
        close.on_click(self._on_close)

        record.observe(self._toggle_record, 'value')
        log.observe(self._toggle_log, 'value')

        self._widget = VBox([
            HBox([listen, close]),
            HBox([record, self._record_output, self._record_overwrite]),
            HBox([log, self._log_output, self._log_overwrite])
        ])

    def _ipython_display_(self):
        display(self._widget)

    def _on_listen(self, button):
        self._player.listen()

    def _on_close(self, button):
        self._player.close()

    def _toggle_record(self, value):
        if value['new']:
            self._player.record_start(
                self._record_output.value,
                overwrite=self._record_overwrite.value
            )
        else:
            self._player.record_stop()

    def _toggle_log(self, value):
        if value['new']:
            self._player.log_start(
                self._log_output.value,
                overwrite=self._log_overwrite.value
            )
        else:
            self._player.log_stop()


class RTDataPlayerMultiWidgetView(RTDataPlayerWidgetView):

    def __init__(self, player):
        super().__init__(player)

        self.stream_log_boxes = []

        for stream in player.streams:
            label = widgets.Label(stream.name)
            log = widgets.ToggleButton(
                value=False,
                description='Log',
                icon='save'
            )
            log_output = widgets.Text(
                value=f'{stream.name}_log.csv',
                description='Output path:',
            )
            log_overwrite = widgets.Checkbox(
                value=False,
                description='Overwrite'
            )
            self.stream_log_boxes.append(HBox([log, log_output, log_overwrite, label]))

        for i, log_box in enumerate(self.stream_log_boxes):
            log_box.children[0].observe(self._toggle_log_stream_factory(i), 'value')

        # add one log box for every stream to the widget of the superclass
        self._widget = VBox([
            *self._widget.children,
            *self.stream_log_boxes,
        ])

    def _toggle_log_stream_factory(self, idx):
        def toggle_log_stream(value):
            if value['new']:
                self._player.log_start_stream(
                    idx,
                    self.stream_log_boxes[idx].children[1].value,
                    overwrite=self.stream_log_boxes[idx].children[2].value
                )
            else:
                self._player.log_stop_stream(idx)

        return toggle_log_stream

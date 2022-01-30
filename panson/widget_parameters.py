import ipywidgets as widgets
from abc import ABC, abstractmethod
from .sonification import Parameter


class WidgetParameter(Parameter, ABC):
    """Base class for all widget parameters."""

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        # name of widget attribute in sonification object
        self.widget_private_name = self.private_name + '_widget'

    def __set__(self, instance, value):
        # update widget and value (through observe callback) atomically
        # this guarantees that value and widget's value are always in sync
        with instance._lock:
            if hasattr(instance, self.widget_private_name):
                widget = getattr(instance, self.widget_private_name)
                # update widget (indirectly updates the instance parameter)
                widget.value = value
            else:
                # this is executed only the first time

                # create widget (without value)
                widget = self._get_ipywidget()

                def on_change(val):
                    # set parameter
                    Parameter.__set__(self, instance, val['new'])

                # bind callback
                widget.observe(on_change, names='value')

                # set value (the callback will be executed)
                widget.value = value
                # assign widget to the instance
                setattr(instance, self.widget_private_name, widget)

    @abstractmethod
    def _get_ipywidget(self):
        """Return ipywidget (without initial value assigned)."""
        pass


class IntSliderParameter(WidgetParameter):

    def __init__(self, min, max, step=1):
        if min >= max:
            raise ValueError(f'min ({min}) cannot be >= max ({max}).')
        self.min = min
        self.max = max

        self.step = step

    def __set__(self, instance, value):
        if not (self.min <= value <= self.max):
            raise ValueError(
                f"value ({value}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self):
        return widgets.IntSlider(
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )


class FloatSliderParameter(WidgetParameter):

    def __init__(self, min, max, step=0.1):
        if min >= max:
            raise ValueError(f'min ({min}) cannot be >= max ({max}).')
        self.min = min
        self.max = max

        self.step = step

    def __set__(self, instance, value):
        if not (self.min <= value <= self.max):
            raise ValueError(
                f"value ({value}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self):
        return widgets.FloatSlider(
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )


class FloatLogSliderParameter(WidgetParameter):

    def __init__(self, min, max, step=0.2, base=10):
        if min >= max:
            raise ValueError(f'min ({min}) cannot be >= max ({max}).')
        self.min = min
        self.max = max

        self.step = step
        self.base = base

    def __set__(self, instance, value):
        if not (self.min <= value <= self.max):
            raise ValueError(
                f"value ({value}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self):
        return widgets.FloatLogSlider(
            base=self.base,
            min=-self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )

# TODO: IntRangeSlider and FloatRangeSlider?


class SelectionParameter(WidgetParameter, ABC):
    """Base class for all selection widget parameters."""

    def __init__(self, options):
        self.options = options

    def __set__(self, instance, value):
        if not (value in self.options):
            raise ValueError(
                f"value ({value}) must be between {self.options}."
            )
        super().__set__(instance, value)


class DropdownParameter(SelectionParameter):

    def _get_ipywidget(self):
        return widgets.Dropdown(
            options=self.options,
            description=self.public_name + ':'
        )


class SelectParameter(SelectionParameter):

    def _get_ipywidget(self):
        return widgets.Select(
            options=self.options,
            description=self.public_name + ':'
        )


class ComboboxParameter(SelectionParameter):

    def _get_ipywidget(self):
        return widgets.Combobox(
            placeholder='Choose option',
            options=self.options,
            description=self.public_name + ':',
            ensure_option=True
        )


class BooleanParameter(WidgetParameter, ABC):
    """Base class for all boolean widget parameters."""

    def __set__(self, instance, value):
        if type(value) != bool:
            raise ValueError(
                f"value ({value}) must be a boolean: got a {type(value)}."
            )
        super().__set__(instance, value)


class ToggleButtonParameter(BooleanParameter):

    def _get_ipywidget(self):
        return widgets.ToggleButton(description=self.public_name)


class CheckboxParameter(BooleanParameter):

    def _get_ipywidget(self):
        return widgets.Checkbox(
            description=self.public_name,
            indent=True
        )

import ipywidgets as widgets
from abc import ABC, abstractmethod

from math import log2


class WidgetParameter(ABC):
    """Base class for all widget parameters."""

    def __set_name__(self, owner, name):
        # save name of the descriptor
        self.public_name = name
        # name of widget attribute in sonification object
        self.widget_private_name = '__' + name + '_widget'

    def __get__(self, instance, owner):
        # get widget's value
        return getattr(instance, self.widget_private_name).value

    def __set__(self, instance, value):
        # update widget atomically:
        #   blocks if a sonification step is being computed
        with instance._lock:
            if hasattr(instance, self.widget_private_name):
                # update widget
                widget = getattr(instance, self.widget_private_name)
                widget.value = value
            else:
                # this is executed only the first time
                # create widget
                widget = self._get_ipywidget(value)
                # assign widget to the sonification instance
                setattr(instance, self.widget_private_name, widget)

    @abstractmethod
    def _get_ipywidget(self, value):
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

    def _get_ipywidget(self, value):
        return widgets.IntSlider(
            value=value,
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

    def _get_ipywidget(self, value):
        return widgets.FloatSlider(
            value=value,
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )


class DbSliderParameter(FloatSliderParameter):

    def __init__(self, step=0.1):
        super().__init__(-90, 0, step)


class MidiSliderParameter(FloatSliderParameter):

    def __init__(self, step=1):
        super().__init__(0, 127, step)


class FloatLogSliderParameter(WidgetParameter):

    def __init__(self, min_exp, max_exp, step=0.2, base=10):
        if min_exp >= max_exp:
            raise ValueError(f'min_exp ({min_exp}) cannot be >= max_exp ({max_exp}).')
        self.min_exp = min_exp
        self.max_exp = max_exp

        self.step = step
        self.base = base

    def __set__(self, instance, value):
        if not (self.base ** self.min_exp <= value <= self.base ** self.max_exp):
            raise ValueError(
                f"value ({value}) must be between min ({self.base ** self.min_exp}) and max ({self.base ** self.max_exp})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value):
        return widgets.FloatLogSlider(
            value=value,
            base=self.base,
            min=self.min_exp,
            max=self.max_exp,
            step=self.step,
            description=self.public_name + ':',
            layout=widgets.Layout(width='98%')
        )


class FreqSliderParameter(FloatLogSliderParameter):

    def __init__(self, min_freq=20, max_freq=20000, step=0.2):
        super().__init__(log2(min_freq), log2(max_freq), step=step, base=2)


class IntRangeSliderParameter(WidgetParameter):

    def __init__(self, min, max, step=1):
        if min >= max:
            raise ValueError(f'min ({min}) cannot be >= max ({max}).')
        self.min = min
        self.max = max

        self.step = step

    def __set__(self, instance, value):
        if not (self.min <= value[0] <= self.max):
            raise ValueError(
                f"value[0] ({value[0]}) must be between min ({self.min}) and max ({self.max})."
            )
        if not (self.min <= value[1] <= self.max):
            raise ValueError(
                f"value[1] ({value[1]}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value):
        return widgets.IntRangeSlider(
            value=value,
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':',
        )


class FloatRangeSliderParameter(WidgetParameter):

    def __init__(self, min, max, step=0.1):
        if min >= max:
            raise ValueError(f'min_exp ({min}) cannot be >= max_exp ({max}).')
        self.min = min
        self.max = max

        self.step = step

    def __set__(self, instance, value):
        if not (self.min <= value[0] <= self.max):
            raise ValueError(
                f"value[0] ({value[0]}) must be between min ({self.min}) and max ({self.max})."
            )
        if not (self.min <= value[1] <= self.max):
            raise ValueError(
                f"value[1] ({value[1]}) must be between min ({self.min}) and max ({self.max})."
            )
        super().__set__(instance, value)

    def _get_ipywidget(self, value):
        return widgets.FloatRangeSlider(
            value=value,
            min=self.min,
            max=self.max,
            step=self.step,
            description=self.public_name + ':'
        )


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

    def _get_ipywidget(self, value):
        return widgets.Dropdown(
            value=value,
            options=self.options,
            description=self.public_name + ':'
        )


class SelectParameter(SelectionParameter):

    def _get_ipywidget(self, value):
        return widgets.Select(
            value=value,
            options=self.options,
            description=self.public_name + ':'
        )


class ComboboxParameter(SelectionParameter):

    def _get_ipywidget(self, value):
        return widgets.Combobox(
            value=value,
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

    def _get_ipywidget(self, value):
        return widgets.ToggleButton(
            value=value,
            description=self.public_name
        )


class CheckboxParameter(BooleanParameter):

    def _get_ipywidget(self, value):
        return widgets.Checkbox(
            value=value,
            description=self.public_name,
            indent=True
        )

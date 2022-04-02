from abc import ABC, abstractmethod
import pandas as pd

# TODO: with this mechanism, the user is forced to use a constructor without
# arguments. We would want the constructor to support parameters.
# This could be done using a builder pattern and passing this instead of the
# Preprocessor type to the framework components.


class Preprocessor(ABC):
    """Subclasses of this class define the stream preprocessors.

    This class is not meant to be instantiated by the user. The user has rather
    to use this class as a preprocessor definition and pass the definition to
    the framework objects (Streams or multi-stream data players) so that they
    will take care of their instantiation.

    Using a class instead of a simple function has the advantage of being able
    of handling contextual data more effectively, as every instance will have
    its separate attribute (the user will define them in the constructor).
    """

    @abstractmethod
    def preprocess(self, row: pd.Series) -> None:
        pass

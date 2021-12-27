from abc import ABC, abstractmethod


class Sonification(ABC):

    def __init__(self):
        pass

    def init(self):
        pass

    def play(self):
        pass

    @abstractmethod
    def play_row(self, row):
        pass




if __name__ == '__main__':
    pass

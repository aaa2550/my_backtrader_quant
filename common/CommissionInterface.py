from abc import abstractmethod, ABC

from enums import SideEnum


class CommissionInterface(ABC):
    @abstractmethod
    def calc(self, side: SideEnum, amount: float):
        pass
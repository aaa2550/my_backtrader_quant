from abc import abstractmethod, ABC

import SideEnum


class CommissionInterface(ABC):
    @abstractmethod
    def calc(self, side: SideEnum, amount: float):
        pass

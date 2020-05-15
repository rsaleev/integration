from enum import Enum


class Color(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3

    @staticmethod
    def list():
        return list(map(lambda c: {'description': c.value, 'number': c.name}, Color))


print(Color.list())

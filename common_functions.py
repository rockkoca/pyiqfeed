from enum import Enum


class Color(Enum):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    BO = '\033[96m'
    UNDERLINE = '\033[4m'


def color_print(text: str, color: Color, bold=False) -> None:
    print(f'{Color.BOLD.value if bold else ""}{color.value}{text}{Color.ENDC.value}')

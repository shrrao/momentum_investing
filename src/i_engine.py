from dataclasses import dataclass
from datetime import date

from pynse import Nse


@dataclass
class IEngine:
    i_nse_handler: Nse #temp
    i_company: str
    i_from_date: date
    i_to_date: date

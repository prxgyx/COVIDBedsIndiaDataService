from states.TamilNadu import TamilNadu
from states.Pune import Pune
from states.Telangana import Telangana
from states.Haryana import Haryana
from sheetsapi.GoogleSheetsAPI import *

TamilNadu().push_data()

Pune().push_data()

Haryana().push_data()

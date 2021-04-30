import logging

from states.AndhraPradesh import AndhraPradesh
from states.Bengaluru import Bengaluru
from states.Haryana import Haryana
from states.Pune import Pune
from states.Rajasthan import Rajasthan
from states.TamilNadu import TamilNadu
from states.Telangana import Telangana

state_classes = [
    AndhraPradesh,
    Bengaluru,
    Haryana,
    Pune,
    Rajasthan,
    TamilNadu,
]

for state_class in state_classes:
    try:
        logging.info(f"Processing state {state_class}")
        state_class().push_data()
    except Exception as e:
        logging.exception(f"Error processing state {state_class}")

logging.info("Done.")

import logging
import argparse

from states.AndhraPradesh import AndhraPradesh
from states.Bengaluru import Bengaluru
from states.Haryana import Haryana
from states.Pune import Pune
from states.Rajasthan import Rajasthan
from states.TamilNadu import TamilNadu
from states.Telangana import Telangana
from states.Chhattisgarh import Chhattisgarh

from states.notification.TelegramBot import TelegramBot

covidbedsbot = TelegramBot()

state_classes = [
    AndhraPradesh,
    Bengaluru,
    Haryana,
    Pune,
    Rajasthan,
    TamilNadu,
    Chhattisgarh,
    Telangana
]

if __name__ == '__main__':
    '''
        Main function to execute
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', help='Supported - prod or test')

    args, unknown = parser.parse_known_args()

    for state_class in state_classes:
        state_name = state_class.__name__
        try:
            logging.info(f"Processing state {state_name}")
            if args.mode == "test":
                state_object = state_class("Test ")
            else:
                state_object = state_class()
            state_object.push_data()
        except Exception as e:
            logging.exception(f"Error processing state {state_name}")
            covidbedsbot.send_message(u'\u274c'+f" Error processing state {state_name}")


    logging.info("Done.")

import logging
import argparse
import time

from states.AndhraPradesh import AndhraPradesh
from states.Bengaluru import Bengaluru
from states.Haryana import Haryana
from states.Nagpur import Nagpur
from states.Nashik import Nashik
from states.Pune import Pune
from states.Rajasthan import Rajasthan
from states.Surat import Surat
from states.TamilNadu import TamilNadu
from states.Telangana import Telangana
from states.Chhattisgarh import Chhattisgarh
from states.Uttarakhand import Uttarakhand
from states.Gandhinagar import Gandhinagar

from states.notification.TelegramBot import TelegramBot

covidbedsbot = TelegramBot()

sim_state_classes = [
    Bengaluru,
    Gandhinagar,
    Haryana,
    Nashik,
    Pune,
    Rajasthan,
    Surat,
    TamilNadu,
    Uttarakhand
]

com_state_classes = [
    Chhattisgarh,
    Nagpur,
    Telangana,
    AndhraPradesh
]

if __name__ == '__main__':
    '''
        Main function to execute
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', help='Supported - prod or test')
    parser.add_argument('--runtype', help='Supported - simple or complex')

    args, unknown = parser.parse_known_args()

    if args.runtype == "complex":
        state_classes = com_state_classes
    else:
        state_classes = sim_state_classes

    for state_class in state_classes:
        state_name = state_class.__name__
        try:
            logging.info(f"Processing state {state_name}")
            if args.mode == "test":
                state_object = state_class("Test ")
            else:
                state_object = state_class()
            state_object.push_data()
            # Wait because throttling in requests
            time.sleep(40)
        except Exception as e:
            logging.exception(f"Error processing state {state_name} - {str(e)}")
            covidbedsbot.send_message(u'\u274c'+f" Error processing state {state_name} - {str(e)}")


    logging.info("Done.")

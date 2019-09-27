import configparser
import logging


def load_configuration(section_name, config_file='./config.ini'):
    configs = configparser.ConfigParser()
    configs.read(config_file)
    config_section = configs[section_name]
    config_params_dict = dict()
    for config_param in config_section:
        config_params_dict[config_param] = config_section[config_param]

    return config_params_dict


def initialize_logger(logging_file: str):
    # datefmt='%d-%b-%y %H:%M:%S'
    f_handler = logging.FileHandler(filename=logging_file, mode='w')
    f_formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
    f_handler.setFormatter(f_formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(f_handler)
    return logger

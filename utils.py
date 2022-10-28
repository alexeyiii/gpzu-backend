import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler('action.log', mode='a')
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(name)s] %(asctime)s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.ERROR)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger
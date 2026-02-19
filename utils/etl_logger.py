import logging


def pipeline_logger():

    logger = logging.getLogger('pipeline_logger') ## get a logger instance by name

    logger.setLevel(logging.INFO) ## it will log info

    if not logger.handlers: ## avoid duplicating handlers (re runnig)

        ## prints to terminal
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        ## how each log line will loke in console
        formatter = logging.Formatter( "%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")

        console_handler.setFormatter(formatter)

        ##attach handler to logger .. 
        logger.addHandler(console_handler)

    return logger










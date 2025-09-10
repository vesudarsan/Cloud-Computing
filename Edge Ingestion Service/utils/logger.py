import logging ,socket
import sys
import os
import json

def setup_logger(name=None):
    # Default level
    container_name = os.getenv("CONTAINER_NAME", socket.gethostname())
    log_level = logging.INFO  
    #  "DEBUG" / "INFO" / "WARNING" / "ERROR

    # Try loading log level from config.json
    config_path = os.path.join(os.path.dirname(__file__), "../config/config.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            level_str = config.get("log_level", "INFO").upper()
            log_level = getattr(logging, level_str, logging.INFO)
    except Exception as e:
        print(f"[Logger] Could not load config for log level: {e}")

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(log_level)
        formatter = logging.Formatter(f'[{container_name}][%(asctime)s] %(levelname)s: %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

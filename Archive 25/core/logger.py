from loguru import logger

def setup_logger():
    logger.add(
        "dea.log",
        format="{time} | {level} | {message}",
        level="INFO"
    )

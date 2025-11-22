import logging


def configure_logging(level=logging.DEBUG):
    """
    Centralized logger configuration for the project.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        force=True  # ensures config always applies
    )

    logger = logging.getLogger(__name__)
    logger.info("Logging system initialized")

    return logger

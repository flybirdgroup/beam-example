import os
from old.config import development


def load_configuration():
    """
    """
    try:
        env = os.environ.get('ENVIRONMENT', None).upper()
    except AttributeError:
        raise EnvironmentError(None)

    if env == 'DEVELOPMENT':
        return development
    else:

        raise EnvironmentError(env)

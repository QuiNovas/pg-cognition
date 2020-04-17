from json import loads
from json.decoder import JSONDecodeError
from os import environ

def validateConfig(requiredOpts, config, defaults={}):
    """Validate the config has all of the required values. For each required value if the option is not set in the config we will attempt to pull it from the environment before throwing an Exception.

    :param requiredOpts: A list of required parameters
    :type requiredOpts: list, required
    :param config: A dictionary of configuration options
    :type config: dict, required
    :param defaults: A dictionary of config options and their defaults to use if missing
    :type defaults: dict, optional

    :returns: Dictionary of configuration options
    :rtype: dict
    """
    for r in requiredOpts:
        if r not in config or config[r] is None:
            if r.upper() in environ:
                try:
                    config[r] = loads(environ[r.upper()])
                except (JSONDecodeError, TypeError):
                    config[r] = environ[r.upper()]
            else:
                raise Exception(f"""Option '{r}' missing in config""")
    for d in defaults:
        if d not in config or config[d] is None:
            if d.upper() in environ:
                try:
                    config[d] = loads(environ[d.upper()])
                except (JSONDecodeError, TypeError):
                    config[d] = environ[d.upper()]
    if "secretsPath" in config:
        config["secretsPath"] = config["secretsPath"].rstrip("/")
    return config

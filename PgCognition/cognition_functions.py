from json import loads
from json.decoder import JSONDecodeError
from os import environ
import boto3
from botocore.exceptions import ClientError

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
            # Prefer env vars over defaults
            if r.upper() in environ:
                # env vars will always be strings, we will cast them if we can
                try:
                    config[r] = loads(environ[r.upper()])
                except (JSONDecodeError, TypeError):
                    config[r] = environ[r.upper()]
            # Try getting from the defaults
            elif r in defaults:
                config[r] = defaults[r]
            else:
                raise Exception(f"""Option '{r}' missing in config""")

    if "secretsPath" in config:
        config["secretsPath"] = config["secretsPath"].rstrip("/")
    return config

def getCallerAccount():
    """Gets account number of entity that called the function using sts service

    :returns: Account number
    :rtype: str
    """
    try:
        return boto3.client('sts').get_caller_identity().get('Account')
    except ClientError as e:
        raise Exception(e.response)

def getCallerFromSecret(secret):
    return secret.split("/")[-1]

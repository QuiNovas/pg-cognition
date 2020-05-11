from json import loads
from json.decoder import JSONDecodeError
from os import environ
from re import match
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

def getAppsyncCaller(event):
    if isinstance(event, list):
        identity = event[0]["identity"]
    else:
        identity = event["identity"]

    user = None
    if "userArn" not in identity:
        user = identity["claims"]["userid"]
    else:
        # Test for an assumed role
        stsRole = match(f'^arn:aws:sts::[0-9]+:assumed-role/(.*)/[0-9]+$', identity["userArn"])
        if stsRole:
            user = stsRole.group(1)
        else:
            user = identity["userArn"].split("/")[-1]
    if user is None: raise Exception("Could not identify caller")
    return user

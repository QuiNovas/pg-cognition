from json import loads
from json.decoder import JSONDecodeError
from os import environ
from .DatabaseClient import DatabaseClient
from .Cognito import Cognito
from . import Schema


def validateConfig(required, config, defaults={}):
    for r in required:
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

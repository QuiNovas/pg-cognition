from string import ascii_letters as letters
import json
from re import match
from time import sleep
import random
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from auroraPrettyParser import parseResults
from .cognition_functions import validateConfig

class DatabaseClient():
    """
    :Keyword Arguments:
        * *event* (``list or dict``) -- event argument to Lambda function
        * *config* (``dict``) -- configuration options
            * *database* -- database name
            * *databaseArn* -- arn of the database
            * *databaseSecretArn* -- (optional) Arn of the database secret to use. Defaults to caller's secret

    Options required in config can be omitted if they exist in os.environ. Options to be taken from the environment should have their names specified in all caps.
    """
    def __init__(self, event=None, config={}):
        self.event = event
        required = ("database", "databaseArn", "databaseSecretArn")
        defaults = {"secretsPath": "rds-db-credentials", "databaseSecretArn": self._getCredentials(event)}
        self.config = validateConfig(required, config, defaults)
        self.client = boto3.client("rds-data")

    def runQuery(self, sql, **kwargs):
        r"""Run a query against an Aurora Serverless Postgresql database

        :param sql: SQL statement to execute
        :type sql: str, required
        :param \**kwargs: See below
        :type \**kwargs: mixed

        :Keyword Arguments:
            * *schema* (``str``) -- schema to use. Boto3 does not currently respect this. Use the full path to your table in the query instead
            * *pretty* (``bool``) -- format the results as a list of dicts, one per row, with the keys as column names, default True
            * *parameters* (``list``) -- a list of parameters to pass to the query
            * *secret* (``str``) -- override config["databaseSecretArn"]
            * *database* (``str``) -- override config["database"]
            * *databaseArn* (``str``) -- override config["databaseArn"]
        """
        schema = None if "schema" not in kwargs else kwargs["schema"]
        pretty = True if "pretty" not in kwargs else kwargs["pretty"]
        parameters = [] if "parameters" not in kwargs else kwargs["parameters"]
        secret = self.config["databaseSecretArn"] if "secret" not in kwargs else kwargs["secret"]
        database = self.config["database"] if "database" not in kwargs else kwargs["database"]
        databaseArn = self.config["databaseArn"] if "databaseArn" not in kwargs else kwargs["databaseArn"]

        res = self.client.execute_statement(
            secretArn=secret,
            schema=schema,
            database=database,
            resourceArn=databaseArn,
            parameters=parameters,
            includeResultMetadata=pretty,
            sql=sql
        )
        if pretty: res = parseResults(res)
        return res

    def resolveAppsyncQuery(self, schema=None):
        """Run a query for each item in the event.

        :param schema: Schema name. Boto3 rds-client doesn't acually honor this right not. It is best to use the full path to your table in your query.
        :type schema: str or None, optional
        :returns: list of dicts or list of list of dicts with column names as keys
        :rtype: list
        """
        try:
            secret = self.config["databaseSecretArn"]
            if isinstance(self.event, list):
                res = []
                for n in range(len(self.event)):
                    result = self.runQuery(
                        self.event[n]["query"],
                        schema=schema,
                        parameters=self.event[n]["parameters"],
                        secret=secret
                    )
                    res.append(parseResults(result))
            else:
                res = self.runQuery(
                    self.event["query"],
                    schema=schema,
                    parameters=self.event["parameters"],
                    secret=secret
                )
                res = parseResults(res)
        except ClientError as e:
            res = e

        return res

    def _getCredentials(self, event):
        """
        Get ARN for the secret containing the RDS credentials for the user who called us.

        Secrets for cognito users are constructed as:
            arn:aws:secretsmanager:{region}:{account}:{secretsPath}/{event["identity"]["email"]}

        We will return the first credential found in the order of:
            1. If caller is a Cognito user
            2. If caller is matched in roleOverrides
            3. If caller is matched in assumedRoleOverrides

        :param event: event from Lambda
        :type event: dict or list
        :returns: The arn of an AWS secret
        :rtype: str
        """

        if isinstance(event, list):
            identity = event[0]["identity"]
        else:
            identity = event["identity"]

        secret = None
        secretBase = f"""arn:aws:secretsmanager:{self.config["region"]}:{self.config["account"]}:{self.config["secretsPath"]}"""
        if "userArn" not in identity:
            secret = f"""{secretBase}/{identity["claims"]["email"]}"""
        elif self.config["roleOverrides"]:
            for role in self.config["roleOverrides"]:
                if identity['userArn'] == self.config["roleOverrides"][role]:
                    secret = f"""{secretBase}/{role}"""
                    break
        elif self.config["assumedRoleOverrides"]:
            for role in self.config["assumedRoleOverrides"]:
                if match(f'^arn:aws:sts::[0-9]+:assumed-role/{role}/[0-9]+$', identity["userArn"]):
                    secret = self.config["assumedRoleOverrides"][role]
                    break
        if secret is None:
            raise Exception("Could not determine secret ARN through cognito claims or IAM overrides")
        return secret

    def createDatabaseUser(self):
        try:
            dbpass = ''.join(random.choice('!@#$%^&*()_-+=1234567890' + letters) for i in range(31))
            email = self.event['user']['email']
            parameters = [
                {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}},
                {'name': 'CURRENT_TIME', 'value': {'stringValue': f'"{datetime.now().isoformat()}"'}}
            ]

            sql = """
                UPDATE pg_cognition.users
                SET
                    status='active',
                    invitation_data=jsonb_set(invitation_data, '{accepted_date}', (:CURRENT_TIME)::JSONB)
                WHERE email = :EMAIL
                RETURNING *;
            """
            newuser = self.runQuery(sql, parameters=parameters)

            if not newuser:
                raise Exception(f"""Application user {email} does not exist.""")

            newuser = newuser[0]
            sql = f"""
                CREATE USER {newuser["id"]} WITH PASSWORD '{dbpass}' IN ROLE {newuser["invitation_data"]["role"]};
            """

            self.runQuery(sql)

            s = boto3.client('secretsmanager')

            try:
                # Delete the credentials if they already exist
                s.delete_secret(
                    SecretId=f'{self.config["secretsPath"]}/{email}',
                    ForceDeleteWithoutRecovery=True
                )
                while s.describe_secret(SecretId=f'{self.config["secretsPath"]}/{email}'):
                    sleep(5)
            except Exception:
                pass

            secretString = {
                "dbClusterIdentifier": self.config["databaseArn"],
                "engine": "postgres",
                "host": self.config["databaseHost"],
                "password": dbpass,
                "port": 5432,
                "username": newuser["id"]
            }

            s.create_secret(
                Name=f'{self.config["secretsPath"]}/{email}',
                Description=f'DB credentials for {email}',
                SecretString=json.dumps(secretString)
            )

        except (Exception, ClientError) as e:
            try:
                sql = f"""
                    UPDATE pg_cognition.users SET status = 'invited' WHERE id = {newuser["id"]};
                    REVOKE {newuser["invitation_data"]["role"]} FROM {newuser["id"]};
                    DROP ROLE {newuser["id"]}
                """

                self.runQuery(sql)
            except Exception:
                pass
            try:
                s.delete_secret(
                    SecretId=f'{self.config["secretsPath"]}/{newuser["email"]}',
                    ForceDeleteWithoutRecovery=True
                )
            except Exception:
                pass
            raise e

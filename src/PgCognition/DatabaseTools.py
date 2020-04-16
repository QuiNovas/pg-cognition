from string import ascii_letters as letters
import json
from re import match
from time import sleep
import random
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from auroraPrettyParser import parseResults
from .functions import validateConfig

class DatabaseTools():
    def __init__(self, event, config={}):
        self.event = event
        required = ("database", "databaseArn", "databaseHost", "databaseSecretArn")
        defaults = {"secretsPath": "rds-db-credentials"}
        self.config = validateConfig(required, config, defaults)
        self.client = boto3.client("rds-data")

    def runQuery(self, sql, **kwargs):
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
        """
        Run a query for each item in the event.

        Kwargs:

            schema -- String specifying the database schema to use. The default is none. Currently boto3 does not
                      respect this kwarg in the execute_statement operation. It is suggested to simply use the full
                      path to your table (SELECT * FROM myschema.mytable) in your resolver
        """
        try:
            secret = self.getCredentials()
            if isinstance(self.event, list):
                res = []
                for n in range(len(self.event)):
                    result = self.client.execute_statement(
                        secretArn=secret,
                        schema=schema,
                        database=self.config["database"],
                        resourceArn=self.config["databaseArn"],
                        parameters=self.event[n]["parameters"],
                        includeResultMetadata=True,
                        sql=self.event[n]["query"]
                    )
                    res.append(parseResults(result))
            else:
                res = self.client.execute_statement(
                    secretArn=secret,
                    schema=schema,
                    database=self.config["database"],
                    resourceArn=self.config["databaseArn"],
                    parameters=self.event["parameters"],
                    includeResultMetadata=True,
                    sql=self.event["query"]
                )
                res = parseResults(res)
        except ClientError as e:
            res = e

        return res

    def getCredentials(self):
        """
        Get RDS credentials for the user who called us.

        Secrets for cognito users are constructed as such:
            arn:aws:secretsmanager:{self.region}:{self.config["account"]}:{self.secretsPath}/self.config["identity"]["email"]

        We will return the first credential found in the order of:
            1. If caller is a Cognito user
            2. If caller is matched in roleOverrides
            3. If caller is matched in assumedRoleOverrides

        """

        if isinstance(self.event, list):
            identity = self.event[0]["identity"]
        else:
            identity = self.event["identity"]

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
                UPDATE global_data.users
                SET
                    status='active',
                    invitation_data=jsonb_set(invitation_data, '{accepted_date}', (:CURRENT_TIME)::JSONB)
                WHERE email = :EMAIL
                RETURNING *;
            """

            newuser = parseResults(
                self.client.execute_statement(
                    secretArn=self.config["databaseSecretArn"],
                    database=self.config["database"],
                    parameters=parameters,
                    resourceArn=self.config["databaseArn"],
                    includeResultMetadata=True,
                    sql=sql
                )
            )

            if not newuser:
                raise Exception(f"""Application user {email} does not exist.""")

            newuser = newuser[0]
            sql = f"""
                CREATE USER {newuser["id"]} WITH PASSWORD '{dbpass}' IN ROLE {newuser["invitation_data"]["role"]};
            """

            self.client.execute_statement(
                secretArn=self.config["databaseSecretArn"],
                database=self.config["database"],
                resourceArn=self.config["databaseArn"],
                sql=sql
            )

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
            removeSql = f"""
                UPDATE global_data.users SET status = 'invited' WHERE id = {newuser["id"]};
                REVOKE {newuser["invitation_data"]["role"]} FROM {newuser["id"]};
                DROP ROLE {newuser["id"]}
            """
            try:
                self.client.execute_statement(
                    secretArn=self.config["databaseSecretArn"],
                    database=self.config["database"],
                    resourceArn=self.config["databaseArn"],
                    includeResultMetadata=True,
                    sql=removeSql
                )
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

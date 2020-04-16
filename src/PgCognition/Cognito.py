import json
from time import sleep
from re import match
from auroraPrettyParser import parseResults
import boto3
from botocore.exceptions import ClientError
from .functions import validateConfig

class Cognito():
    def __init__(self, event, config):
        self.event = event
        required = ("database", "databaseArn", "databaseSecret", "region")
        self.config = validateConfig(required, config)

    def PreTokenGeneration(self, extraClaims=[]):
        email = self.event["request"]["userAttributes"]["email"]
        sql = f"""
            SELECT
                t.name AS tenant,
                global_data.tenantrole('{email}', t.name::TEXT) AS role
            FROM auth_data.users u
            JOIN auth_data.tenants t ON u.tenant_id=t.id
            WHERE u.email='{email}';
        """

        c = boto3.client('rds-data')
        claims = parseResults(
            c.execute_statement(
                secretArn=self.config["databaseSecret"],
                database=self.config["database"],
                parameters=[],
                resourceArn=self.config["databaseArn"],
                includeResultMetadata=True,
                sql=sql
            )
        )[0]

        self.event["response"] = {
            "claimsOverrideDetails": {
                "claimsToAddOrOverride": {
                    "tenant": claims["tenant"],
                    "role": claims["role"],
                }
            }
        }

        for claim in extraClaims:
            self.event["response"]["claimsToAddOrOverride"][claim] = extraClaims[claim]

        return self.event

    def PreAuth(self):
        email = self.event['request']['userAttributes']['email']
        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT email, status
                FROM global_data.users u
                WHERE u.email=:EMAIL AND status='active'
            LIMIT 1;
        """

        client = boto3.client('rds-data')
        result = client.execute_statement(
            secretArn=self.config["databaseArn"],
            database=self.config["database"],
            parameters=parameters,
            resourceArn=self.config["databaseArn"],
            includeResultMetadata=True,
            sql=sql
        )

        if "records" not in result or not result["records"]:
            sleep(1)  # No brute force
            raise Exception(f"User {email} failed to login. User may not exist in application or may be in the wrong status.")

        return self.event

    def PreSignup(self):
        email = self.event['request']['userAttributes']['email']
        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT email, status
                FROM global_data.users
                WHERE email=:EMAIL AND status='invited'
            LIMIT 1;
        """

        client = boto3.client('rds-data')
        result = parseResults(
            client.execute_statement(
                secretArn=self.config["databaseArn"],
                database=self.config["database"],
                parameters=parameters,
                resourceArn=self.config["databaseArn"],
                includeResultMetadata=True,
                sql=sql
            )
        )

        if not "records" not in result:
            sleep(1)  # No brute force
            raise Exception(f"Cannot complete signup for {email}. Contact your administrator.")

        return self.event

    def PostSignup(self, createUserArn):
        client = boto3.client('rds-data')

        email = self.event['request']['userAttributes']['email']

        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT * FROM global_data.users
            WHERE email = :EMAIL AND status='invited';
        """

        try:
            user = parseResults(
                client.execute_statement(
                    secretArn=self.config["databaseArn"],
                    database=self.config["database"],
                    parameters=parameters,
                    resourceArn=self.config["databaseArn"],
                    includeResultMetadata=True,
                    sql=sql
                )
            )
            if not user:
                raise Exception(f"Could not update user {email} to active. Contact your administrator.")

            if not match('^[a-zA-Z0-9_]+$', user[0]["invitation_data"]["tenant"]): raise Exception("Bad tenant name.")
            if not match('^[a-zA-Z0-9_]+$', user[0]["invitation_data"]["role"]): raise Exception("Bad role name")
        except ClientError as e:
            raise Exception(e.response)
        except Exception as e:
            raise e

        l = boto3.client('lambda')
        l.invoke(
            FunctionName=createUserArn,
            InvocationType='Event',
            Payload=json.dumps({'user': user})
        )

        return self.event

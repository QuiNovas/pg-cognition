import random
import json
from datetime import datetime, date
from re import match
from time import time_ns as ns
from string import ascii_letters as letters
from .DatabaseClient import DatabaseClient
from .cognition_functions import getCallerFromSecret

class Cognito():
    """Methods useful in building cognito hooks

    :param config: A database configuration suitable to be passed to DatabaseClient()
    :type config: dict, optional
    :param dbCLient: An instance of DatabaseClient that will be used for queries
    :type dbCLient: PgCognition.DatabaseClient, optional
    :param event: AWS Lambda event
    :type event: dict or list, optional
    :returns: An instance of Cognito()
    :rtype: PgCognition.Cognito

    You can only pass one of config or dbClient. dbClient must be a configured instance of DatabaseClient. If using DatabaseClient then event can be omitted and dbClient.event will be used, otherwise dbClient.event will be replaced with self.event
    """

    def __init__(self, event=None, config=None, dbClient=None, client_type="instance"):
        if config is not None and dbClient is not None:
            raise Exception("You must pass only ONE of config or dbClient")
        if isinstance(dbClient, DatabaseClient):
            if event is not None:
                dbClient.event = event
            self.dbClient = dbClient
            self.event = dbClient.event
            self.config = dbClient.config
        else:
            self.dbClient = DatabaseClient(event=event, config=config, client_type=client_type)
            self.event = event

    def AddClaims(self, extraClaims={}):
        """
        Adds claims to Cognito user and returns the object's event. Intended to be used with Cognito Pre Token Generation.
        Default is to add role and tenant.

        :param extraClaims: Extra claims to add to jwt
        :type extraClaims: dict, optional
        :returns: self.event updated with claims
        :rtype: dict
        """

        email = self.event["request"]["userAttributes"]["email"]
        sql = f"""
            SELECT
                t.name AS tenant,
                pg_cognition.tenantrole('{email}', t.name::TEXT) AS role,
                u.id AS userid
            FROM cognition.users u
            JOIN cognition.tenants t ON u.tenant_id=t.id
            WHERE u.email='{email}';
        """

        claims = self.dbClient.runQuery(sql)[0]

        self.event["response"] = {
            "claimsOverrideDetails": {
                "claimsToAddOrOverride": {
                    "tenant": claims["tenant"],
                    "role": claims["role"],
                    "userid": claims["userid"]
                }
            }
        }

        for claim in extraClaims:
            self.event["response"]["claimsToAddOrOverride"][claim] = extraClaims[claim]

        return self.event

    def UserIsActive(self):
        """
        Returns a bool indicating if a user exists and is active.
        Useful for Cognito Pre Authentication hook

        :returns: bool indicating if user is in active state
        :rtype: bool
        """

        email = self.event['request']['userAttributes']['email']
        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT email, status
                FROM cognition.users u
                WHERE u.email=:EMAIL AND status='active'
            LIMIT 1;
        """

        return bool(self.dbClient.runQuery(sql, parameters=parameters, pretty=False))

    def UserIsInvited(self):
        """
        Returns a bool indicating if a user has been invited or not

        If called by Cognito Post Signup then DatabaseClient.createDatabaseUser({"user": user})
        can be called afterward to create the user in the database. If using AWS Lambda then the
        Lambda function that calls createDatabaseUser() should be invoked async after this method
        since it can take longer than the 5 timeout that applies to Cognito hooks.

        If called by Cognito Pre Signup then we will simply prove that a user has been invited
        before continuing with the signup process.

        :returns: bool indicating if a user has been invited
        :rtype: bool
        """

        email = self.event['request']['userAttributes']['email']

        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT * FROM pg_cognition.users
            WHERE email = :EMAIL AND status='invited';
        """

        return bool(self.dbClient.runQuery(sql, parameters=parameters))

    def userExists(self, email):
        if self.dbClient.client_type == "serverless":
            parameters = [{'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}]
            sql = "SELECT id FROM pg_cognition.users WHERE email = :EMAIL;"
        else:
            parameters = {"EMAIL": email}
            sql = "SELECT id FROM pg_cognition.users where email = %(EMAIL)s;"

        return bool(self.dbClient.runQuery(sql, parameters))

    def createApplicationUser(self, caller=None):
        userid = ''.join(random.choice(letters.lower()) for i in range(12)) + '_' + datetime.now().strftime("%Y%m%d%H%M%S") + str(ns())
        dbRole = f"""{self.event["parameters"]['tenant']}_{self.event["parameters"]['role']}"""
        if self.dbClient.client_type == "serverless":
            secret = self.dbClient.getCredentials()
            caller = getCallerFromSecret(secret)

        inviteData = {
            "invited_by": caller,
            "invite_date": str(datetime.now()),
            "role": dbRole,
            "tenant": self.event["parameters"]["tenant"]
        }

        assert match('^[a-zA-Z0-9_]+$', self.event["parameters"]["tenant"]) and match('^[a-zA-Z0-9_]+$', self.event["parameters"]["role"]), \
            "Invalid tenant or role name"

        if self.userExists(self.event["parameters"]["email"]): raise Exception("User already exists.")

        if self.dbClient.client_type == "serverless":
            parameters = [
                {'name': 'EMAIL', 'value': {'stringValue': f'{self.event["parameters"]["email"]}'}},
                {'name': 'FIRSTNAME', 'value': {'stringValue': f'{self.event["parameters"]["firstname"]}'}},
                {'name': 'LASTNAME', 'value': {'stringValue': f'{self.event["parameters"]["lastname"]}'}},
                {'name': 'DBROLE', 'value': {'stringValue': f'{dbRole}'}},
                {'name': 'APPROLE', 'value': {'stringValue': f'{self.event["parameters"]["role"]}'}},
                {'name': 'USERID', 'value': {'stringValue': f'{userid}'}},
                {'name': 'TENANT', 'value': {'stringValue': f'{self.event["parameters"]["tenant"]}'}},
                {'name': 'INVITEDATA', 'value': {'stringValue': f'{json.dumps(inviteData)}'}}
            ]
            sql = """
                INSERT INTO pg_cognition.users (id, email, first_name, last_name, status, invitation_data, tenant_id) VALUES (
                    :USERID,
                    :EMAIL,
                    :FIRSTNAME,
                    :LASTNAME,
                    'invited',
                   :INVITEDATA::jsonb,
                    (SELECT id FROM global_data.tenants WHERE name = :TENANT)
                )
                RETURNING
                    *,
                    (SELECT displayname from global_data.tenants WHERE name = :TENANT) AS tenant_name;
            """
        elif self.dbClient == "instance":
            parameters = {
                "EMAIL": self.event["parameters"]["email"],
                "FIRSTNAME": self.event["parameters"]["firstname"],
                "LASTNAME": self.event["parameters"]["lastname"],
                "DBROLE": self.event["parameters"]["role"],
                "APPROLE": self.event["parameters"]["role"],
                "USERID": userid,
                "TENANT": self.event["parameters"]["tenant"],
                "INVITEDATA": json.dumps(inviteData)
            }
            sql = """
                INSERT INTO global_data.users (id, email, first_name, last_name, status, invitation_data, tenant_id) VALUES (
                    %(USERID)s,
                    %(EMAIL)s,
                    %(FIRSTNAME)s,
                    %(LASTNAME)s,
                    'invited',
                    %(INVITEDATA)s::jsonb,
                    (SELECT id FROM pg_cognition.tenants WHERE name = %(TENANT)s)
                )
                RETURNING
                    *,
                    (SELECT displayname from global_data.tenants WHERE name = %(TENANT)) AS tenant_name;
            """

        try:
            newUser = self.dbClient.runQuery(sql, parameters)[0]
            return newUser

        except Exception as e:
            if self.dbClient.client_type == "serverless":
                parameters = {
                    {'name': 'USERID', 'value': {'stringValue': f'{userid}'}},
                }
                sql = "DELETE FROM pg_cognition.users WHERE id = :USERID;"
            elif self.dbClient.client_type == "instance":
                parameters = {
                    "USERID": userid
                }
                sql = "DELETE FROM pg_cognition.users WHERE id = %(USERID)"
            try:
                self.dbClient.runQuery(sql, parameters)
            except Exception:
                pass
            raise e

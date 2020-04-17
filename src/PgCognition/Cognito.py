from . import validateConfig
from .DatabaseClient import DatabaseClient

class Cognito():
    def __init__(self, event, config):
        self.event = event
        required = ("database", "databaseArn", "databaseSecret", "region")
        self.config = validateConfig(required, config)
        self.dbClient = DatabaseClient(config=self.config)

    def AddClaims(self, extraClaims=[]):
        """
        Adds claims to Cognito user and returns the object's event.
        Intended to be used with Cognito Pre Token Generation hook
        """

        email = self.event["request"]["userAttributes"]["email"]
        sql = f"""
            SELECT
                t.name AS tenant,
                pg_cognition.tenantrole('{email}', t.name::TEXT) AS role
            FROM auth_data.users u
            JOIN auth_data.tenants t ON u.tenant_id=t.id
            WHERE u.email='{email}';
        """

        claims = self.dbClient.runQuery(sql)[0]

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

    def UserIsActive(self):
        """
        Returns a bool indicating if a user exists and is active.
        Useful for Cognito Pre Authentication hook
        """

        email = self.event['request']['userAttributes']['email']
        parameters = [
            {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}}
        ]

        sql = """
            SELECT email, status
                FROM pg_cognition.users u
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

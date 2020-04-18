from .DatabaseClient import DatabaseClient

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

    def __init__(self, event=None, config=None, dbClient=None):
        if config is not None and dbClient is not None:
            raise Exception("You must pass only ONE of config or dbClient")
        if dbClient is not None:
            if event is not None:
                dbClient.event = event
            self.dbClient = dbClient
            self.event = dbClient.event
            self.config = dbClient.config
        else:
            self.dbClient = DatabaseClient(event=event, config=config)
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

        :returns: bool indicating if user is in active state
        :rtype: bool
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

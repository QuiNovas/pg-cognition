from string import ascii_letters as letters
import json
from re import match
from time import sleep
import random
from datetime import datetime
import boto3
import psycopg2
from psycopg2.extras import DictCursor
from botocore.exceptions import ClientError
from auroraPrettyParser import parseResults
from .cognition_functions import validateConfig, getCallerAccount

class DatabaseClient():
    """
    :Keyword Arguments:
        * *client_type* -- (``str``) -- "serverless" for Aurora Serverless, otherwise "instance". Defaults to "instance"
        * *event* (``list or dict``) -- event argument from Lambda function
        * *config* (``dict``) -- configuration options
            * *region* -- (``str``, optional) aws region. Defaults to us-east-1
            * *database* -- (``str``) database name
            * *databaseArn* -- (``str``) arn of the database
            * *databaseSecretArn* -- (optional) Arn of the database secret to use. Defaults to caller's secret
            * *account* -- (optional) Account number where secrets live. Defaults to account number resolved by cognition_functions.getCallerAccount()
            * *secretsPath* -- (optional) Path to AWS secrets. Defaults to rds-db-credentials/
            * *roleOverrides* -- (``dict``, optional) a dictionary of {<role arn>: <secret name>} to match for auth
            * *assumedRoleOverrides* -- (``dict``, optional) a dictionary of {<role name>: <secret name>} to match for auth. Used to auth assumed roles (cli users for instance)
    :returns: An instance of PgCognition.DatabaseClient
    :rtype: PgCognition.DatabaseClient

    :Environment: Options required in config can be omitted if they exist in os.environ. Options to be taken from the environment should have their names specified in all caps.

    *Example with a standard Postgres database*

    ..  code-block:: python

        from PgCognition import DatabaseClient

        config = {
            "dbname": "mydb",
            "user": "myuser",
            "host": "localhost",
            "password": "password"
        }
        c = DatabaseClient(config=config, client_type="instance")
        res = c.runQuery("SELECT * FROM mytable", switch_role="less_privileged_role")

    *Example with a Aurora Serverless*

    ..  code-block:: python

        from PgCognition import DatabaseClient

        config = {
            "database": "mydb",
            "account": 12345678,
            "databaseArn": "arn:aws:rds:us-east-1:123456789:db:mydb",
            "assumedRoleOverrides": {"administrator": "root-secret", "developer": "root-secret"},
            "region": "us-east-1",
            "secretsPath": "rds-db-credentials"
        }
        c = DatabaseClient(event=event, config=config)
        res = c.runQuery("SELECT * FROM mytable", switch_role="less_privileged_role")
    """

    def __init__(self, event=None, config={}, client_type="instance"):
        assert client_type in ("serverless", "instance"), "kwarg client_type must be one of 'serverless' or 'instance'"
        self.event = event

        if client_type == "serverless":
            defaults = {"account": getCallerAccount(), "region": "us-east-1", "secretsPath": "rds-db-credentials"}
            required = ("dbname", "databaseArn", "account", "region")
            self.config = validateConfig(required, config, defaults=defaults)
            self.client = boto3.client("rds-data")
            self.client_type = "serverless"
        else:
            required = ("dbname", "host", "user", "password", "port")
            defaults = {"port": 5432}
            self.config = validateConfig(required, config, defaults=defaults)
            self.client = None
            self.connect()
            self.client_type = "instance"
            self.commit = self.client.commit

    def connect(self):
        self.client = psycopg2.connect(
            dbname=self.config["dbname"],
            user=self.config["user"],
            host=self.config["host"],
            password=self.config["password"],
            port=self.config["port"]
        )

    def close(self):
        """Close the database connection for client_type of "instance"

        :returns: None
        :rtype: None

        This will throw an AttributeError if client_type is not "instance"

        """

        if self.client_type == "instance":
            self.client.close()

    def runQuery(self, sql, **kwargs):
        """Run an SQL query HELLO

        This method is a convenience wrapper for runInstanceQuery and runServerlessQuery
        The method called whose return value is returned from this method and whose arguments
        should be passed will depend on self.client_type


        :Keyword Arguments:
            * *pretty* (``bool``) -- format the results as a list of dicts, one per row, with the keys as column names, default True
            * *fetch_results* (``bool``) -- If False don't return any results. Default is True
            * *parameters* (``list``) -- a list of parameters to pass to the query in psycopg2's format
            * *switch_role* (``string``) -- Execute as <role>. Only available for "instance" client_type
            * *commit* (``bool``) -- Commit directly after query. Only available for "instance" client_type
            * *secret* (``str``) -- override config["databaseSecretArn"] for serverless clients
            * *database* (``str``) -- override config["database"] for serverless clients
            * *databaseArn* (``str``) -- override config["databaseArn"] for serverless clients
            * *schema* (``str``) -- schema to use for serverless clients

        :returns: List or Dictionary of query results
        :rtype: list or dict

        Boto3 does not currently respect the schema argument. Use the full path to your table in the query instead. if pretty=True then
        query results will be returned after being formatted by auroraPrettyParser.parseResults (serverless clients) or a dict
        using pyscopg2.extras.DictCursor cast to a dict (instance clients). Otherwise results will be returned using the underlying client's
        default formatting. To use with Appsync you would pretty much always want pretty=True

        *Example with a standard Postgres database*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            res = c.runQuery("SELECT * FROM mytable", switch_role="less_privileged_role")
        """

        if isinstance(self.client, psycopg2.extensions.connection):
            res = self._runInstanceQuery(sql, **kwargs)
        else:
            res = self._runServerlessQuery(sql, **kwargs)
        return res

    def _runInstanceQuery(self, sql, **kwargs):
        r"""Run a query against an Postgresql database

        :param sql: SQL statement to execute
        :type sql: str, required

        :Keyword Arguments:
            * *pretty* (``bool``) -- format the results as a list of dicts, one per row, with the keys as column names, default True
            * *fetch_results* (``bool``) -- If False don't return any results. Default is True
            * *parameters* (``list``) -- a list of parameters to pass to the query in psycopg2's format
            * *switch_role* (``string``) -- Run "SET ROLE <switch_role>" before executing the query to escalate/deescalate privileges
            * *commit* (``bool``) -- Commit directly after query
            * *reset_auth* (``bool``) -- Reset auth after query
        """
        commit = True if "commit" not in kwargs else kwargs["commit"]
        assert commit in (True, False), "commit kwarg must be a boolean"

        reset_auth = True if "reset_auth" not in kwargs else kwargs["reset_auth"]
        assert reset_auth in (True, False), "reset_auth kwarg must be a boolean"

        if "switch_role" in kwargs and kwargs["switch_role"] is not None:
            switch_role = True
            sql = f"""SET ROLE "{kwargs["switch_role"]}"; {sql}"""
        else:
            switch_role = False
        pretty = True if "pretty" not in kwargs else kwargs["pretty"]
        parameters = {} if "parameters" not in kwargs else kwargs["parameters"]
        cursor_type = DictCursor if pretty else None
        fetch_results = True if "fetch_results" not in kwargs else kwargs["fetch_results"]
        c = self.client.cursor(cursor_factory=cursor_type)
        try:
            c.execute(
                sql,
                parameters
            )
            if fetch_results:
                if pretty: r = [dict(x) for x in c.fetchall()]
                else: r = [list(x) for x in c.fetchall()]
            else:
                r = None
        finally:
            if commit: self.client.commit()
            if reset_auth and switch_role: c.execute("RESET SESSION AUTHORIZATION;")
        return r

    def _runServerlessQuery(self, sql, **kwargs):
        r"""Run a query against an Aurora Serverless Postgresql database

        :param sql: SQL statement to execute
        :type sql: str, required

        :Keyword Arguments:
            * *schema* (``str``) -- schema to use. Boto3 does not currently respect this. Use the full path to your table in the query instead
            * *pretty* (``bool``) -- format the results as a list of dicts, one per row, with the keys as column names, default True
            * *parameters* (``list``) -- a list of parameters to pass to the query
            * *secret* (``str``) -- override config["databaseSecretArn"]
            * *database* (``str``) -- override config["database"]
            * *databaseArn* (``str``) -- override config["databaseArn"]

        :returns: AWS Aurora rds-data response, optionally formatted with auroraPrettyParser
        :rtype: list or dict
        """
        schema = "" if "schema" not in kwargs or kwargs["schema"] is None else kwargs["schema"]
        pretty = True if "pretty" not in kwargs else kwargs["pretty"]
        parameters = [] if "parameters" not in kwargs else kwargs["parameters"]
        database = self.config["database"] if "database" not in kwargs else kwargs["database"]
        databaseArn = self.config["databaseArn"] if "databaseArn" not in kwargs else kwargs["databaseArn"]
        if "secret" in kwargs:
            secret = kwargs["secret"]
        elif "databaseSecretArn" in self.config:
            secret = self.config["databaseSecretArn"]
        else:
            secret = self.getSecretFromIdentity()

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

    def resolveAppsyncQuery(self, schema="", secretArn=None, client_type=None, switch_role=None, event=None):
        """Run a query for each item in event

        This is a wrapper for DatabaseClient.resolveInstanceAppsyncQuery and DatabaseClient.resolveServerlessAppsyncQuery and
        will return the result of one of these two methods, depending on whether client_type is "serverless" or "instance".
        See PgCognition.DatabaseClient.resolveInstanceAppsyncQuery and PgCognition.DatabaseClient.resolveServerlessAppsyncQuery

        :Keyword Arguments:
            * *schema* (``str``) -- Schema name. Boto3 rds-client doesn't acually honor this right not. It is best to use the full path to your table in your query. Only used if client_type = "serverless"
            * *secretArn* (``str``) -- Override databaseSecretArn from config. Only used if client_type = "serverless"
            * *switch_role* (``str``) -- Excute query as specific user. The current connection must use a user with permission to assume this role. Only used if client_type = "instance"

        :returns: Query results from PgCognition.DatabaseClient.resolveInstanceAppsyncQuery or PgCognition.DatabaseClient.resolveServerlessAppsyncQuery
        :rtype: dict or list

        *Example of an AWS Lambda function used as an Appsync datasource with a serverless database*

        .. code-block:: python

            from os import environ
            from PgCognition import DatabaseClient

            def handler(event, context):
                config = {
                    "database": environ["DATABASE"],
                    "account": environ["ACCOUNT"],
                    "databaseArn": environ["DATABASE_ARN"],
                    "assumedRoleOverrides": {"administrator": "root", "developer": "root"}, # This allows our awscli users calling Appsync
                    "region": environ["AWS_REGION"],
                    "secretsPath": "rds-db-credentials"
                }
                dbClient = DatabaseClient(event=event, config=config, client_type="serverless")
                return dbClient.resolveAppsyncQuery()
        """
        if event is not None: self.event = event
        if client_type is None:
            client_type = self.client_type
        if client_type == "serverless":
            res = self._resolveServerlessAppsyncQuery(schema=schema, secretArn=secretArn)
        else:
            res = self._resolveInstanceAppsyncQuery(switch_role=switch_role)
        return res

    def _resolveInstanceAppsyncQuery(self, switch_role=None):
        """Run a query for each item in the event.

        :Keyword Arguments:
            * *switch_role* (``str``) -- Excute query as specific user. The current connection must use a user with permission to assume this role

        :returns: list of dicts or list of list of dicts with column names as keys
        :rtype: list
        """

        if isinstance(self.event, list):
            result = []
            for n in range(len(self.event)):
                result.append(
                    self._runInstanceQuery(
                        self.event[n]["query"],
                        parameters=self.event[n]["parameters"],
                        switch_role=switch_role,
                        commit=True,
                        reset_auth=True
                    )
                )
        else:
            result = self._runInstanceQuery(
                self.event["query"],
                parameters=self.event["parameters"],
                switch_role=switch_role,
                commit=True,
                reset_auth=True
            )
        return result

    def _resolveServerlessAppsyncQuery(self, schema="", secretArn=None):
        """Run a query for each item in the event.

        :Keyword Arguments:
            * *schema* (``str``) -- Schema name. Boto3 rds-client doesn't acually honor this right not. It is best to use the full path to your table in your query.
            * *secretArn* (``str``) -- Override databaseSecretArn from config

        :returns: list of dicts or list of list of dicts with column names as keys
        :rtype: list
        """
        try:
            if secretArn is not None:
                secret = secretArn
            elif "databaseSecretArn" in self.config and secretArn is None:
                secret = self.config["databaseSecretArn"]
            else:
                secret = self.getSecretFromIdentity()

            if isinstance(self.event, list):
                res = []
                for n in range(len(self.event)):
                    result = self._runServerlessQuery(
                        self.event[n]["query"],
                        schema=schema,
                        parameters=self.event[n]["parameters"],
                        secret=secret
                    )
                    res.append(result)
            else:
                res = self._runServerlessQuery(
                    self.event["query"],
                    schema=schema,
                    parameters=self.event["parameters"],
                    secret=secret
                )
        except ClientError as e:
            res = e

        return res

    def getSecretFromIdentity(self):
        """
        Get ARN for the secret containing the RDS credentials for the user who called us.

        Secrets for cognito users are constructed as:
            arn:aws:secretsmanager:{region}:{account}:{secretsPath}/{event["identity"]["email"]}

        We will return the first credential found in the order of:
            1. If caller is a Cognito user
            2. If caller is matched in roleOverrides
            3. If caller is matched in assumedRoleOverrides

        :returns: The arn of an AWS secret
        :rtype: str
        """

        if isinstance(self.event, list):
            identity = self.event[0]["identity"]
        else:
            identity = self.event["identity"]

        secret = None
        secretBase = f"""arn:aws:secretsmanager:{self.config["region"]}:{self.config["account"]}:secret:{self.config["secretsPath"]}"""
        if "userArn" not in identity:
            secret = f"""{secretBase}/{identity["claims"]["email"]}"""
        elif self.config["roleOverrides"]:
            for arn in self.config["roleOverrides"]:
                if identity['userArn'] == arn:
                    secret = f"""{secretBase}/{self.config["roleOverrides"][arn]}"""
                if secret is not None: break
        elif self.config["assumedRoleOverrides"]:
            for roleName in self.config["assumedRoleOverrides"]:
                if match(f'^arn:aws:sts::[0-9]+:assumed-role/{roleName}/[0-9]+$', identity["userArn"]):
                    secret = f"""{secretBase}/{self.config["assumedRoleOverrides"][roleName]}"""
                if secret is not None: break
        if secret is None:
            raise Exception("Could not determine secret ARN through cognito claims or IAM overrides")
        return secret

    def createCognitionUser(self, email=None):
        """Creates a database user that is tied to an application user in cognition.users table. Normally run as part of Cognito's PostSignup hook.

        :Keyword Arguments:
            * *email* (``str``) -- Override the email address from self.event["user"]["email"]

        :returns: Dictionary containing new user's database credentials and corresponding user from cognition.users table
        :rtype: dict

        This method does not just create a database user/role. It marries a user in the cognition.users table to a new database user. If
        the email address passed to the method does not exist in the cognition.users table then an Exception will be raised. If you want to
        create arbitrary users use PgCognition.DatabaseClient.createRole method.
        """

        dbpass = ''.join(random.choice('!@#$%^&*()_-+=1234567890' + letters) for i in range(31)) if self.client_type == "serverless" else "NULL"
        email = email if email is not None else self.event["user"]["email"]
        try:
            if self.client_type == "serverless":
                parameters = [
                    {'name': 'EMAIL', 'value': {'stringValue': f'{email}'}},
                    {'name': 'CURRENT_TIME', 'value': {'stringValue': f'"{datetime.now().isoformat()}"'}}
                ]

                sql = """
                    UPDATE cognition.users
                    SET
                        status='active',
                        invitation_data=jsonb_set(invitation_data, '{accepted_date}', (:CURRENT_TIME)::JSONB)
                    WHERE email = :EMAIL
                    RETURNING *;
                """
            else:
                parameters = {
                    "CURRENT_TIME": datetime.now().isoformat(),
                    "EMAIL": email
                }
                sql = f"""
                    UPDATE pg_cognition.users
                    SET
                        status='active'
                        invitation_data=jsonb_set(invitation_data, '{{accepted_date}}', (%(CURRENT_TIME)s)::JSONB)
                    WHERE email = %(EMAIL)s RETURNING *;
                """

            newuser = self.runQuery(sql, parameters=parameters)

            if not newuser:
                raise Exception(f"""Application user {email} does not exist.""")

            newuser = newuser[0]
            self.createRole(newuser["id"], newuser["invitation_data"]["role"], password=dbpass)
            if self.client_type == "serverless":
                newuser["secret"] = self.createSecret(newuser["userid"], newuser["email"], dbpass, upsert=True)
            else:
                newuser["password"] = dbpass

            return newuser
        except (Exception, ClientError) as e:
            try:
                sql = f"""
                    UPDATE pg_cognition.users SET status = 'invited' WHERE id = {newuser["id"]};
                    REVOKE {newuser["invitation_data"]["role"]} FROM {newuser["id"]};
                    DROP ROLE {newuser["id"]};
                """
                self.runQuery(sql)
            except Exception:
                pass
            if self.client_type == "serverless":
                try:
                    self.deleteSecret(newuser["email"], wait=False)
                except Exception:
                    pass
            raise e

    def deleteSecret(self, user, wait=True):
        """Deletes an AWS Secret
        :param user:  Email address of the user to remove the secret for
        :type user: str

        :Keyword Arguments:
            * *wait* (``bool``) -- Block until delete is complete
        """
        s = boto3.client("secretsmanager")
        s.delete_secret(
            SecretId=f'{self.config["secretsPath"]}/{user}',
            ForceDeleteWithoutRecovery=True
        )
        if wait:
            while s.describe_secret(SecretId=f'{self.config["secretsPath"]}/{user}'):
                sleep(5)

    def createSecret(self, userid, email, dbpass, upsert=True):
        """Creates an AWS Secret to use for database credentials

        :param userid: The database username to put in the secret
        :type userid: str
        :param email: The cognito email address for the database user
        :type email: str
        :param dbpass: Database password to store
        :type dbpass: str

        :Keyword Arguments:
            * *upsert* (``bool``) -- If True (default) then update the secret if a secret exists for this email address.

        :returns: ARN of secret
        :rtype: str

        If upsert=False and a secret already exists for the email address an Exception will be raised.
        """
        s = boto3.client('secretsmanager')
        secretString = {
            "dbClusterIdentifier": self.config["databaseArn"],
            "engine": "postgres",
            "host": self.config["databaseHost"],
            "password": dbpass,
            "port": 5432,
            "username": userid
        }

        if s.describe_secret(SecretId=f'{self.config["secretsPath"]}/{email}'):
            if not upsert:
                raise Exception(f"""Secret {self.config["secretsPath"]}/{email} already exists. Use upsert=True to update or delete first""")
            arn = s.update_secret(
                SecretId=f'{self.config["secretsPath"]}/{email}',
                Description=f'DB credentials for {email}',
                SecretString=json.dumps(secretString)
            )["ARN"]
        else:
            arn = s.create_secret(
                Name=f'{self.config["secretsPath"]}/{email}',
                Description=f'DB credentials for {email}',
                SecretString=json.dumps(secretString)
            )["ARN"]

        return arn

    def cloneSchema(self, src, dest):
        """Clones a schema with all objects and permissions

        :param src: The source schema to clone
        :type src: str
        :param dest: The name of the destination schema
        :type dest: str
        :returns: None
        :rtype: None

        *Example*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            c.cloneSchema("tenant_template", "new_tenant")
        """
        self.client.runQuery(f"""SELECT cognition.clone_schema('{src}', '{dest}', True, False)""", pretty=False, commit=True)

    def createRole(self, role, group='NULL', password="NULL"):
        """Creates a role, optionally with login, optionally in a group

        :param role: Name of new role
        :type role: str

        :Keyword Arguments:
            * *group* (``str``) -- Group to add role to
            * *password* (``str``) -- Give role login privilege with provided password

        :returns: None
        :rtype: None

        If password kwarg is provided then user will have login privileges with provided password, otherwise the role will not have login.
        If group kwarg is provided the role will be created inside of that group.

        *Example adding a user role to a tenant for an instance type client*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            c.createRole("myuser", "my_tenant_users")
        """
        sql = f"""SELECT cognition.createrole({role}, {group}, '{password}')"""
        self.client.runQuery(sql, pretty=False, commit=True)

    def getTenantRole(self, email, tenant):
        """Get the role for a Cognito user by email within a specific tenant

        :param email: Email address of user
        :type email: str
        :param tenant: Name of tenant to retrieve user's role within
        :type role: str

        :returns: The user's role within a tenant
        :rtype: str


        *Example adding a user role to a tenant for an instance type client*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            role = c.getTenantRole("myser@foo.com", "my_tenant")
        """

        result = self.runQuery(f"""SELECT * FROM cognition.tenantrole('{email}', '{tenant}')""", pretty=True)
        return result[0]["tenantrole"]

    def getTenants(self, identifier, identifier_type='email'):
        """Get the tenants a user belongs to. Either by email, or database role/user

        :param identifier: Email address or database role of user
        :type identifier: str

        :Keyword Arguments:
            * *identifier_type* (``str``) -- Cognito email address or "dbuser"

        :returns: The user's tenants
        :rtype: list

        *Example adding a user role to a tenant for an instance type client*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            role = c.getTenantRole("myser@foo.com", "my_tenant")
        """

        result = self.runQuery(f"""SELECT * FROM cognition.gettenants('{identifier}', '{identifier_type}')""", pretty=True)
        return [x["gettenants"] for x in result]

    def groupsOf(self, username):
        """Get the database groups a database user belongs to

        :param user: Database username
        :type user: str
        :returns: A list of database groups a database user belongs to
        :rtype: list

        *Example*

        ..  code-block:: python

            from PgCognition import DatabaseClient

            config = {
                "dbname": "mydb",
                "user": "myuser",
                "host": "localhost",
                "password": "password"
            }
            c = DatabaseClient(config=config, client_type="instance")
            groups = c.groupsOf(""myuserid")
        """

        result = self.runQuery(f"""SELECT * FROM cognition.groupsof('{username}')""", pretty=True)
        return [x["groupsof"] for x in result]

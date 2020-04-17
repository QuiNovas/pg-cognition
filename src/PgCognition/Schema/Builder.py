#!/usr/bin/env python3.7
from . import \
    CLONE_SCHEMA, \
    USERS_TABLE, \
    TENANTS_TABLE, \
    CREATE_ROLE, \
    GET_TENANTS, \
    GROUPS_OF, \
    POLICIES, \
    GRANTS
from ..cognition_functions import validateConfig
from .. import DatabaseClient

class Builder():
    def __init__(self, config={}):
        required = ("database", "databaseArn", "databaseHost", "databaseSecretArn")
        defaults = {}
        self.config = validateConfig(required, config, defaults)
        self.dbClient = DatabaseClient(config)

    def createCognitionSchema(self):
        queries = [
            "CREATE SCHEMA pg_cognition;",
            "CREATE SCHEMA pg_cognition;",
            "CREATE SCHEMA shared_extensions;",
            f"""CREATE EXTENSION "uuid-ossp" WITH SCHEMA shared_extensions;""",
            CREATE_ROLE,
            USERS_TABLE,
            TENANTS_TABLE,
            GET_TENANTS,
            GROUPS_OF,
            CLONE_SCHEMA,
            "SELECT pg_cognition.createrole('application_admins', NULL, NULL);",
            "GRANT USAGE ON SCHEMA pg_cognition to application_admins;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO application_admins;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA pg_cognition GRANT EXECUTE ON FUNCTIONS TO application_admins;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA pg_cognition GRANT ALL ON TABLES to application_admins;",
            GRANTS,
            "DROP SCHEMA IF EXISTS tenant_template CASCADE;",
            "CREATE SCHEMA tenant_template;",
            "GRANT USAGE ON SCHEMA tenant_template TO application_admins;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA tenant_template GRANT ALL PRIVILEGES ON TABLES TO application_admins;"
        ]
        for p in POLICIES:
            queries.append(p)

        for q in queries:
            self.dbClient.runQuery(q)

    def removeCognitionSchema(self):
        queries = [
            "DROP POLICY admins_tenants_select on pg_cognition.tenants",
            "DROP POLICY admins_users_all on pg_cognition.users",
            "DROP POLICY admins_tenants_update on pg_cognition.tenants",
            "DROP POLICY users_tenants_select on pg_cognition.tenants",
            "DROP POLICY users_user_update on pg_cognition.users",
            "DROP POLICY application_admins_users on pg_cognition.users",
            "DROP POLICY application_admins_tenants on pg_cognition.tenants",
            "DROP ROLE tenant_users",
            "DROP ROLE tenant_admins",
            "DROP ROLE application_admins"
            "DROP SCHEMA pg_cognition CASCADE",
        ]
        for q in queries:
            self.dbClient.runQuery(q)

#!/usr/bin/env python3.7
from . import \
    CLONE_SCHEMA, \
    USERS_TABLE, \
    TENANTS_TABLE, \
    CREATE_ROLE, \
    GET_TENANTS, \
    GROUPS_OF, \
    TENANT_ROLE, \
    POLICIES, \
    GRANTS
from .. import DatabaseClient

class Builder():
    def __init__(self, config={}, client=None):
        if client is not None and isinstance(client, DatabaseClient):
            self.dbClient = client
        else:
            self.dbClient = DatabaseClient(config=config)

    def createCognitionSchema(self):
        queries = [
            "CREATE SCHEMA cognition;",
            f"""CREATE EXTENSION "uuid-ossp" WITH SCHEMA public;""",
            CREATE_ROLE,
            TENANTS_TABLE,
            USERS_TABLE,
            GET_TENANTS,
            GROUPS_OF,
            TENANT_ROLE,
            "SELECT cognition.createrole('application_owner', NULL, NULL);",
            "GRANT USAGE ON SCHEMA cognition to application_owner;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO application_owner;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA cognition GRANT EXECUTE ON FUNCTIONS TO application_owner;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA cognition GRANT ALL ON TABLES to application_owner;",
            GRANTS,
            "DROP SCHEMA IF EXISTS tenant_template CASCADE;",
            "CREATE SCHEMA tenant_template;",
            "GRANT USAGE ON SCHEMA tenant_template TO application_owner;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA tenant_template GRANT ALL PRIVILEGES ON TABLES TO application_owner;",
            CLONE_SCHEMA

        ]
        for p in POLICIES:
            queries.append(p)

        for q in queries:
            self.dbClient.runQuery(q.replace("%", "%%"), pretty=False, commit=True)

    def removeCognitionSchema(self):
        queries = [
            "DROP POLICY admins_tenants_select on cognition.tenants",
            "DROP POLICY admins_users_all on cognition.users",
            "DROP POLICY admins_tenants_update on cognition.tenants",
            "DROP POLICY users_tenants_select on cognition.tenants",
            "DROP POLICY users_user_update on cognition.users",
            "DROP POLICY application_owner_users on cognition.users",
            "DROP POLICY application_owner_tenants on cognition.tenants",
            "DROP SCHEMA cognition CASCADE",
            "DROP ROLE tenant_users",
            "DROP ROLE tenant_admins",
            "DROP ROLE application_owner"
        ]
        for q in queries:
            self.dbClient.runQuery(q, pretty=False, commit=True)

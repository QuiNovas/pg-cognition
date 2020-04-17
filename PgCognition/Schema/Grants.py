GRANTS = """
    SELECT pg_cognition.createrole('tenant_admins', NULL, NULL);
    GRANT USAGE ON SCHEMA pg_cognition TO tenant_admins;
    GRANT SELECT, INSERT, DELETE, UPDATE ON TABLE pg_cognition.users TO tenant_admins;
    GRANT SELECT, UPDATE (displayname) ON TABLE pg_cognition.tenants to tenant_admins;
    ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO tenant_admins;

    SELECT pg_cognition.createrole('tenant_users', NULL, NULL);
    GRANT USAGE ON SCHEMA pg_cognition TO tenant_users;
    GRANT UPDATE (
        first_name,
        last_name,
        user_preferences,
    ) ON TABLE pg_cognition.users TO tenant_users;
    GRANT SELECT ON TABLE pg_cognition.users to tenant_users;
    GRANT SELECT ON TABLE pg_cognition.tenants to tenant_users;
    GRANT SELECT ON TABLE pg_cognition.tenants to batch;
    GRANT USAGE ON SCHEMA shared_extensions to tenant_users;
    GRANT USAGE ON SCHEMA shared_extensions to tenant_admins;
    GRANT USAGE ON SCHEMA shared_extensions to application_admins;
    ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO tenant_users;
    ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO tenant_admins;
    ALTER DEFAULT PRIVILEGES IN SCHEMA shared_extensions GRANT EXECUTE ON FUNCTIONS TO application_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.gettenants TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.gettenants TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION pg_cognition.gettenants TO GROUP application_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.groupsof TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.groupsof TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION pg_cognition.groupsof TO GROUP application_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.tenantrole TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION pg_cognition.tenantrole TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION pg_cognition.tenantrole TO GROUP application_admins;
"""

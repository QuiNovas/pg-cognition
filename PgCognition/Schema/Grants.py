GRANTS = """
    SELECT cognition.createrole('tenant_admins', NULL, NULL);
    GRANT USAGE ON SCHEMA cognition TO tenant_admins;
    GRANT SELECT, INSERT, DELETE, UPDATE ON TABLE cognition.users TO tenant_admins;
    GRANT SELECT, UPDATE (displayname) ON TABLE cognition.tenants to tenant_admins;

    SELECT cognition.createrole('tenant_users', NULL, NULL);
    GRANT USAGE ON SCHEMA cognition TO tenant_users;
    GRANT UPDATE (
        first_name,
        last_name,
        user_preferences
    ) ON TABLE cognition.users TO tenant_users;
    GRANT SELECT ON TABLE cognition.users to tenant_users;
    GRANT SELECT ON TABLE cognition.tenants to tenant_users;
    GRANT EXECUTE ON FUNCTION cognition.gettenants TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION cognition.gettenants TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION cognition.gettenants TO GROUP application_owner;
    GRANT EXECUTE ON FUNCTION cognition.groupsof TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION cognition.groupsof TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION cognition.groupsof TO GROUP application_owner;
    GRANT EXECUTE ON FUNCTION cognition.tenantrole TO GROUP tenant_admins;
    GRANT EXECUTE ON FUNCTION cognition.tenantrole TO GROUP tenant_users;
    GRANT EXECUTE ON FUNCTION cognition.tenantrole TO GROUP application_owner;
"""

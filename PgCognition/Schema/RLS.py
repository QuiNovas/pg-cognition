ADMIN_TENANTS_POLICY = """
    -- admins can select tenants that they belong to
    CREATE POLICY admins_tenants_select ON cognition.tenants
    FOR SELECT
    TO tenant_admins
    USING ( name in (SELECT cognition.gettenants(current_user::text)));

    -- admins can update the tenant that they belong to
    CREATE POLICY admins_tenants_update ON cognition.tenants
    FOR UPDATE
    TO tenant_admins
    USING ( name in (SELECT cognition.gettenants(current_user::text)))
    WITH CHECK ( name in (SELECT cognition.gettenants(current_user::text)));
"""

ADMIN_USERS_POLICY = """
    -- Admins can do anything on a user that belongs to their tenant
    CREATE POLICY admins_users_all ON cognition.users
    FOR ALL
    TO tenant_admins
    USING (
        tenant_id in (SELECT id FROM cognition.tenants)
    );
"""

USER_TENANTS_POLICY = """
    -- allow users to see their own tenant info
    CREATE POLICY users_tenants_select ON cognition.tenants
    FOR SELECT
    TO tenant_users
    USING (
        name IN (SELECT cognition.gettenants(current_user::text))
    )
"""

USER_USERS_POLICY = """
    --- users can see their own data
    CREATE POLICY users_user_select ON cognition.users
    FOR SELECT
    TO tenant_users
    USING (id = current_user AND status = 'active');

    -- users can update their own data
    CREATE POLICY users_user_update ON cognition.users
    FOR UPDATE
    TO tenant_users
    USING (id = current_user AND status = 'active');
"""

APPLICATION_ADMIN_USERS_POLICY = """
    CREATE POLICY application_owner_users ON cognition.users
    FOR ALL
    TO application_owner
    USING (true)
    WITH CHECK (true);
"""

APPLICATION_ADMIN_TENANTS_POLICY = """
    CREATE POLICY application_owner_tenants ON cognition.tenants
    FOR ALL
    TO application_owner
    USING (true)
    WITH CHECK (true);
"""

POLICIES = [
    APPLICATION_ADMIN_USERS_POLICY,
    APPLICATION_ADMIN_TENANTS_POLICY,
    USER_USERS_POLICY,
    USER_TENANTS_POLICY,
    ADMIN_USERS_POLICY,
    ADMIN_TENANTS_POLICY
]

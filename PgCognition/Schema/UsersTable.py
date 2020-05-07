USERS_TABLE = """
    CREATE TABLE cognition.users
    (
        id VARCHAR(31) PRIMARY KEY,
        email CHARACTER VARYING UNIQUE,
        first_name CHARACTER VARYING,
        last_name CHARACTER VARYING,
        user_preferences JSONB DEFAULT '[]',
        status VARCHAR (30) DEFAULT NULL,
        expires_at timestamp,
        invitation_data JSONB,
        tenant_id uuid REFERENCES cognition.tenants (id) ON DELETE CASCADE
    );
    CREATE INDEX users_user_preferences_gin_idx ON cognition.users USING gin(user_preferences);
    ALTER TABLE cognition.users ENABLE ROW LEVEL SECURITY;
"""

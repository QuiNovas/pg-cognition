USERS_TABLE = """
    CREATE TABLE users
    (
        id VARCHAR(31) PRIMARY KEY,
        email CHARACTER VARYING UNIQUE,
        first_name CHARACTER VARYING,
        last_name CHARACTER VARYING,
        user_preferences JSONB DEFAULT '[]',
        status VARCHAR (30) DEFAULT NULL,
        expires_at timestamp,
        invitation_data JSONB,
        tenant_id uuid REFERENCES tenants (id) ON DELETE CASCADE,
    );
    CREATE INDEX users_user_preferences_gin_idx ON users USING gin(user_preferences);
    ALTER TABLE users ENABLE ROW LEVEL SECURITY;
"""

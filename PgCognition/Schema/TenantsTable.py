TENANTS_TABLE = """
    CREATE TABLE tenants
    (
        id uuid PRIMARY KEY DEFAULT shared_extensions.uuid_generate_v4() NOT NULL,
        name CHARACTER VARYING UNIQUE NOT NULL,
        displayname CHARACTER VARYING NOT NULL,
    );
    ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
    -- Special tenant for admins
    INSERT INTO pg_cognition.tenants VALUES (DEFAULT, 'application_admins', 'Super Admin');
"""

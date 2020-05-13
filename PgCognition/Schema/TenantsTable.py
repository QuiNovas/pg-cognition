TENANTS_TABLE = """
    CREATE TABLE cognition.tenants
    (
        id uuid PRIMARY KEY DEFAULT public.uuid_generate_v4() NOT NULL,
        name CHARACTER VARYING UNIQUE NOT NULL,
        displayname CHARACTER VARYING NOT NULL
    );
    ALTER TABLE cognition.tenants ENABLE ROW LEVEL SECURITY;
    -- Special tenant for admins
    INSERT INTO cognition.tenants VALUES (DEFAULT, 'application_owner', 'Super Admin');
"""

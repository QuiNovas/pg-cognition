GET_TENANTS = """
   CREATE OR REPLACE FUNCTION cognition.gettenants(identifier text, usertype text default 'email')
    RETURNS setof text AS $$
    DECLARE
        rolename pg_roles.rolname%TYPE;
        username text;
        alltenants cognition.tenants.name%TYPE;
    BEGIN
        IF usertype = 'email' THEN
            username := (SELECT id FROM cognition.users WHERE email = identifier);
        ELSEIF usertype = 'dbuser' THEN
            username := identifier;
        ELSE
            RAISE EXCEPTION 'usertype must be one of dbuser or email';
        END IF;

        FOR rolename IN
            SELECT  a.rolname FROM  pg_authid a
            WHERE  pg_has_role(username, a.oid, 'member')
                AND  a.rolname != username
                AND REGEXP_REPLACE(a.rolname, '(_admin(s)?|_user(s)?)$', '' ) IN (SELECT name from cognition.tenants)
            LOOP
            RETURN NEXT (SELECT regexp_replace(rolename, '(_admin(s)?|_user(s)?)$', '' )) AS rolename;
        END LOOP;
        RETURN;
    END;
    $$ LANGUAGE PLPGSQL VOLATILE;
"""

GROUPS_OF = """
    CREATE OR REPLACE FUNCTION cognition.groupsof(username text)
        RETURNS setof text AS $$
            DECLARE rolename pg_roles.rolname%TYPE;
            BEGIN
                FOR rolename IN
                    SELECT  a.rolname FROM  pg_authid a
                    WHERE  pg_has_role(username, a.oid, 'member') AND  a.rolname != username
                LOOP
                RETURN NEXT rolename;
                END LOOP;
                RETURN;
        END;
    $$ LANGUAGE PLPGSQL VOLATILE;
"""

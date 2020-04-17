GROUPS_OF = """
    CREATE OR REPLACE FUNCTION pg_cognition.groupsof(username text)
        RETURNS setof text AS $$
            DECLARE rolename pg_roles.rolname%TYPE;
            BEGIN
                FOR rolename IN
                    SELECT rolname FROM pg_user
                        JOIN pg_auth_members ON (pg_user.usesysid=pg_auth_members.member)
                        JOIN pg_roles ON (pg_roles.oid=pg_auth_members.roleid)
                    WHERE
                    pg_user.usename=username LOOP
                    RETURN NEXT rolename;
                END LOOP;
                RETURN;
        END;
    $$ LANGUAGE PLPGSQL VOLATILE;
"""

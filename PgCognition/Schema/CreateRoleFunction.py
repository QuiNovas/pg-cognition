CREATE_ROLE = r"""
    CREATE OR REPLACE FUNCTION cognition.createrole(_groupname NAME, _ingroup NAME DEFAULT NULL, _pass TEXT DEFAULT NULL)
        RETURNS void AS  $$
            BEGIN
               IF NOT EXISTS (
                      SELECT rolname AS name FROM pg_roles WHERE rolname = _groupname
                      UNION SELECT usename AS name from pg_user WHERE usename = _groupname
                  ) THEN
                      IF _pass IS NOT NULL THEN
                         EXECUTE 'CREATE USER ' || _groupname || E' WITH PASSWORD \'' || _pass || E'\'';
                      ELSE
                          EXECUTE  'CREATE ROLE ' || _groupname;
                      END IF;
                      IF _ingroup IS NOT NULL THEN
                          EXECUTE 'GRANT ' || _ingroup || ' TO ' || _groupname;
                      END IF;
                      RAISE NOTICE  'ROLE % CREATED', _groupname;
               ELSE
                   RAISE NOTICE  'ROLE % already exists. Skipping', _groupname;
               END IF;
               RETURN;
            END;
        $$
        LANGUAGE plpgsql VOLATILE;
"""

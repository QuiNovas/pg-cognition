SWITCH_ROLE = """
    CREATE OR REPLACE FUNCTION cognition.switch_role(user_email TEXT)
    RETURNS void AS  $$
    DECLARE
      u name;
      BEGIN
        u := (SELECT id from cognition.users t WHERE t.email = user_email);
        EXECUTE 'SET ROLE "' || u || '"'
        RETURN;
      END;
    $$
    LANGUAGE plpgsql VOLATILE;
"""

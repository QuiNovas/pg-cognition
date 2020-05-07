TENANT_ROLE = r"""
  CREATE OR REPLACE FUNCTION cognition.tenantrole(useremail text, tenant text)
    RETURNS text AS $$

    SELECT (
      SELECT  REGEXP_REPLACE(groupsof, '(.*_)(admin|user)s$', '\2') as role
      FROM cognition.groupsof(
        (SELECT id FROM cognition.users WHERE email=useremail)
      )
      WHERE groupsof LIKE tenant || '_%'
    ) AS role WHERE (SELECT name FROM cognition.tenants WHERE name = tenant) IS NOT NULL;
  $$ LANGUAGE SQL;
"""

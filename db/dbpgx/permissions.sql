-- Set system permissions.
-- Revoke all privileges from public and this role.
REVOKE ALL ON system.events FROM PUBLIC;
REVOKE ALL ON system.events FROM "server";

GRANT USAGE ON SCHEMA system TO "server";

-- Grant only SELECT and INSERT (append-only).
GRANT SELECT, INSERT ON system.events TO "server";

GRANT SELECT, INSERT, UPDATE ON system.projection_versions TO "server";

-- Prevent future privilege leakage on new tables.
ALTER DEFAULT PRIVILEGES IN SCHEMA system
  REVOKE ALL ON TABLES FROM "server";

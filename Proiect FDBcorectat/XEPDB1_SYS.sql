GRANT CREATE ANY VIEW TO FDBO;
ALTER USER FDBO QUOTA UNLIMITED ON USERS;

BEGIN
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'localhost',
    ace => xs$ace_type(
      privilege_list => xs$name_list('connect'),
      principal_name => 'FDBO',
      principal_type => xs_acl.ptype_db
    )
  );
END;
/


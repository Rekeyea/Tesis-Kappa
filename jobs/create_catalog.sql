CREATE CATALOG kappa_catalog WITH (
    'type' = 'jdbc',
    'base-url' = 'jdbc:postgresql://postgres:5432',
    'default-database' = 'flink_metadata',
    'username' = 'flink',
    'password' = 'flinkpassword'
    
);

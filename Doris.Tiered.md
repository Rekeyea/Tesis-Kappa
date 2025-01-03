```sql
CREATE RESOURCE IF NOT EXISTS "minio" PROPERTIES (
    "type"="s3",
    "s3.endpoint" = "http://172.20.5.2:9000",
    "s3.region" = "us-east-1",
    "s3.root.path" = "/",
    "s3.access_key" = "minioadmin",
    "s3.secret_key" = "minioadmin",
    "s3.connection.maximum" = "50",
    "s3.connection.request.timeout" = "3000",
    "s3.connection.timeout" = "1000",
    "s3.bucket" = "cold"
);
```

```sql
CREATE STORAGE POLICY coldness PROPERTIES (
    "storage_resource" = "minio",
    "cooldown_ttl" = "10" -- in seconds
);
```

```sql
CREATE DATABASE testing;
```

```sql
USE testing;
```

```sql
CREATE TABLE IF NOT EXISTS Test (
    V INTEGER NOT NULL
) PROPERTIES (
    "storage_policy" = "coldness"
);
```
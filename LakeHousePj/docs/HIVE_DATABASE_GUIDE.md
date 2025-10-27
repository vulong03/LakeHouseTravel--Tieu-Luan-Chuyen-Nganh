# Hive Database Guide

## Overview
This guide provides comprehensive information about the Hive Metastore and HiveServer2 setup in the LakeHouse project.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      LakeHouse Stack                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐         ┌───────────────┐                │
│  │  HiveServer2 │◄────────┤ Hive Metastore│                │
│  │  Port: 10000 │         │  Port: 9083   │                │
│  └──────┬───────┘         └───────┬───────┘                │
│         │                          │                         │
│         │                          │                         │
│         ▼                          ▼                         │
│  ┌────────────────────────────────────────┐                │
│  │      PostgreSQL (Metadata Store)        │                │
│  │      Database: metastore_db             │                │
│  │      Port: 5432                         │                │
│  └────────────────────────────────────────┘                │
│                                                               │
│  ┌────────────────────────────────────────┐                │
│  │      MinIO (S3-Compatible Storage)      │                │
│  │      Buckets: bronze, silver, gold      │                │
│  │      Port: 9000                         │                │
│  └────────────────────────────────────────┘                │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Services

### 1. Hive Metastore
- **Container**: `lakehouse_hive_metastore`
- **Port**: 9083
- **Purpose**: Manages metadata for Hive tables, schemas, and partitions
- **Backend**: PostgreSQL database (`metastore_db`)

### 2. HiveServer2
- **Container**: `lakehouse_hive_server2`
- **Port**: 10000 (Thrift), 10002 (Web UI)
- **Purpose**: Provides JDBC/ODBC interface for querying Hive tables
- **Authentication**: NOSASL (no authentication for local development)

### 3. PostgreSQL Metadata Database
- **Container**: `lakehouse_postgres`
- **Database**: `metastore_db`
- **User**: `lakehouse_user`
- **Tables**: 84 metadata tables

## Fixed Issues

### Problem: HiveServer2 Failed to Start
**Error Message:**
```
RuntimeException: The dir: /tmp/hive on HDFS should be writable. 
Current permissions are: rwxr-xr-x
```

**Root Cause:**
HiveServer2 was trying to create scratch directories on HDFS, but we're using MinIO/S3 instead of HDFS.

**Solution:**
Added the following configurations to `hive-site.xml`:

```xml
<!-- Set default filesystem to S3/MinIO -->
<property>
    <name>fs.defaultFS</name>
    <value>s3a://bronze</value>
</property>

<!-- Set scratch directory on S3 -->
<property>
    <name>hive.exec.scratchdir</name>
    <value>s3a://bronze/tmp/hive</value>
</property>

<!-- Disable authorization checks that require HDFS -->
<property>
    <name>hive.security.authorization.enabled</name>
    <value>false</value>
</property>
```

## Configuration Files

### 1. `hive-site.xml`
Located at: `LakeHousePj/hive/init/hive-site.xml`

Key configurations:
- PostgreSQL connection settings
- S3/MinIO credentials and endpoint
- HiveServer2 settings (port, authentication)
- Filesystem paths for S3 storage

### 2. `docker-compose.yml`
Services defined:
- `hive-metastore`: Metastore service
- `hive-server2`: HiveServer2 service

## Health Check Script

### Running the Health Check
```powershell
# From LakeHousePj directory
.\scripts\check-hive-db.ps1
```

### What It Checks
1. Container status (healthy/unhealthy)
2. Hive Metastore port (9083)
3. HiveServer2 port (10000)
4. PostgreSQL metadata database access
5. Hive databases listing
6. Table operations (CREATE TABLE test)

### Expected Output
```
[1/6] Checking container status...
  [OK] Both containers running and healthy

[2/6] Checking Hive Metastore (port 9083)...
  [OK] Hive Metastore is running

[3/6] Checking HiveServer2 (port 10000)...
  [OK] HiveServer2 is running

[4/6] Checking PostgreSQL metadata database...
  [OK] PostgreSQL metadata database is accessible
  [INFO] Total metadata tables: 84

[5/6] Listing Hive databases...
  [OK] Successfully connected to HiveServer2
  Available databases:
    - bronze
    - default

[6/6] Testing table operations...
  [OK] Table operations working correctly
```

## Common Commands

### Connect to HiveServer2 (Interactive)
```bash
docker exec -it lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl'
```

### Execute SQL Query
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e 'SHOW DATABASES;'
```

### Show All Tables in a Database
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e 'SHOW TABLES IN bronze;'
```

### Create a Table
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e "CREATE TABLE test_table (id INT, name STRING) STORED AS PARQUET;"
```

### Describe a Table
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e 'DESCRIBE bronze.bookings;'
```

### Query Data
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e 'SELECT * FROM bronze.bookings LIMIT 10;'
```

## Checking Metadata in PostgreSQL

### Connect to PostgreSQL
```bash
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db
```

### List All Hive Metadata Tables
```sql
\dt
```

### Check Hive Databases
```sql
SELECT * FROM "DBS";
```

### Check Hive Tables
```sql
SELECT * FROM "TBLS";
```

### Check Table Columns
```sql
SELECT * FROM "COLUMNS_V2";
```

## Viewing Logs

### HiveServer2 Container Logs
```bash
docker logs lakehouse_hive_server2 --tail 100 -f
```

### HiveServer2 Application Logs
```bash
docker exec lakehouse_hive_server2 tail -f /tmp/hive/hive.log
```

### Hive Metastore Logs
```bash
docker logs lakehouse_hive_metastore --tail 100 -f
```

## Troubleshooting

### Issue: Connection Refused on Port 10000
**Check if HiveServer2 is running:**
```bash
docker exec lakehouse_hive_server2 nc -z localhost 10000
```

**Check logs for errors:**
```bash
docker exec lakehouse_hive_server2 tail -100 /tmp/hive/hive.log
```

### Issue: "Could not open client transport with JDBC Uri"
**Solution:** Use `auth=noSasl` parameter in JDBC URL:
```
jdbc:hive2://localhost:10000/default;auth=noSasl
```

### Issue: Container is Unhealthy
**Check health status:**
```bash
docker ps | grep hive
```

**Restart the service:**
```bash
docker-compose restart hive-server2
```

### Issue: Metastore Connection Failed
**Check if PostgreSQL is running:**
```bash
docker exec lakehouse_postgres pg_isready -U lakehouse_user -d metastore_db
```

**Verify database exists:**
```bash
docker exec lakehouse_postgres psql -U lakehouse_user -l
```

## JDBC Connection from External Applications

### JDBC URL Format
```
jdbc:hive2://<host>:10000/<database>;auth=noSasl
```

### Example (from host machine)
```
jdbc:hive2://localhost:10000/default;auth=noSasl
```

### Example (from another container)
```
jdbc:hive2://hive-server2:10000/default;auth=noSasl
```

### Maven/Gradle Dependencies
```xml
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>4.0.0</version>
</dependency>
```

## Storage Locations

### Warehouse Directory
Default location for Hive tables:
```
s3a://bronze/warehouse
```

### Scratch Directory
Temporary files during query execution:
```
s3a://bronze/tmp/hive
```

### Local Scratch Directory
Local temporary files:
```
/tmp/hive
```

## Performance Tuning

### Useful Hive Settings
```sql
-- Enable compression
SET hive.exec.compress.output=true;

-- Set parallel execution
SET hive.exec.parallel=true;

-- Optimize joins
SET hive.auto.convert.join=true;
```

## Security Notes

⚠️ **Important**: Current setup uses `NOSASL` authentication which is suitable for **development only**.

For production environments, consider:
- Enable Kerberos authentication
- Use SSL/TLS for connections
- Implement proper access controls
- Use strong passwords and credentials
- Enable audit logging

## Maintenance

### Backup Metadata
```bash
docker exec lakehouse_postgres pg_dump -U lakehouse_user metastore_db > hive_metadata_backup.sql
```

### Restore Metadata
```bash
docker exec -i lakehouse_postgres psql -U lakehouse_user metastore_db < hive_metadata_backup.sql
```

### Clean Up Old Data
```sql
-- Drop unused tables
DROP TABLE IF EXISTS default.old_table;

-- Remove old partitions
ALTER TABLE bronze.bookings DROP IF EXISTS PARTITION (year=2020);
```

## References

- [Apache Hive Documentation](https://hive.apache.org/)
- [Hive JDBC Driver](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC)
- [S3A FileSystem](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [Hive Metastore Schema](https://cwiki.apache.org/confluence/display/Hive/Design#Design-MetastoreERDiagram)

## Support

For issues or questions:
1. Run the health check script: `.\scripts\check-hive-db.ps1`
2. Check container logs for errors
3. Verify configurations in `hive-site.xml`
4. Review this guide for troubleshooting steps


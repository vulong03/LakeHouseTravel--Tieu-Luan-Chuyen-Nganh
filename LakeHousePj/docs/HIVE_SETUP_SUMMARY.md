# Hive Setup Summary

## Status: ✅ FULLY OPERATIONAL

Date: October 27, 2025  
Completed by: AI Assistant

---

## What Was Fixed

### Problem
HiveServer2 was failing to start with the error:
```
RuntimeException: The dir: /tmp/hive on HDFS should be writable. 
Current permissions are: rwxr-xr-x
```

### Root Cause
HiveServer2 was attempting to create scratch directories on HDFS, but the system uses MinIO/S3 storage instead of HDFS.

### Solution Applied
Modified `LakeHousePj/hive/init/hive-site.xml` to add the following configurations:

1. **Default Filesystem**: Set to S3/MinIO
   ```xml
   <property>
       <name>fs.defaultFS</name>
       <value>s3a://bronze</value>
   </property>
   ```

2. **Scratch Directories**: Configured for S3 storage
   ```xml
   <property>
       <name>hive.exec.scratchdir</name>
       <value>s3a://bronze/tmp/hive</value>
   </property>
   ```

3. **Security Settings**: Disabled HDFS-dependent authorization
   ```xml
   <property>
       <name>hive.security.authorization.enabled</name>
       <value>false</value>
   </property>
   ```

---

## Current System Status

### Services Running
| Service | Container | Port | Status | Health |
|---------|-----------|------|--------|--------|
| Hive Metastore | lakehouse_hive_metastore | 9083 | Running | ✅ Healthy |
| HiveServer2 | lakehouse_hive_server2 | 10000 | Running | ✅ Healthy |
| HiveServer2 Web UI | lakehouse_hive_server2 | 10002 | Running | ✅ Healthy |
| PostgreSQL | lakehouse_postgres | 5432 | Running | ✅ Healthy |
| MinIO | lakehouse_minio | 9000 | Running | ✅ Healthy |

### Databases Available
- `bronze` - Contains raw data tables
- `default` - Default Hive database

### Tables in Bronze Database
1. `raw_booking_hotels_detail`
2. `raw_booking_hotels_list`
3. `raw_booking_hotels_reviews`
4. `raw_tiktok_post_comments`
5. `raw_tiktok_post_metadata`
6. `raw_tiktok_video_links`

### PostgreSQL Metadata
- **Database**: `metastore_db`
- **Metadata Tables**: 84 tables
- **Status**: Fully operational

---

## Quick Start Commands

### 1. Health Check
```powershell
# Run comprehensive health check
.\scripts\check-hive-db.ps1
```

### 2. Connect to Hive (Interactive)
```bash
docker exec -it lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl'
```

### 3. Show All Databases
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' \
  -e 'SHOW DATABASES;'
```

### 4. Show Tables in Bronze
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/bronze;auth=noSasl' \
  -e 'SHOW TABLES;'
```

### 5. Describe a Table
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/bronze;auth=noSasl' \
  -e 'DESCRIBE raw_booking_hotels_list;'
```

### 6. Query Data
```bash
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/bronze;auth=noSasl' \
  -e 'SELECT * FROM raw_booking_hotels_list LIMIT 10;'
```

---

## Files Modified

### 1. `LakeHousePj/hive/init/hive-site.xml`
**Changes:**
- Added filesystem configuration for S3/MinIO
- Configured scratch directories
- Added local directories settings
- Disabled HDFS-dependent authorization

**Status:** ✅ Updated and tested

### 2. `LakeHousePj/scripts/check-hive-db.ps1`
**Status:** ✅ Created (New file)

**Purpose:** Comprehensive health check script for Hive services

**Features:**
- Container status verification
- Port connectivity checks
- Database listing
- Table operations testing
- Metadata database verification

---

## Test Results

### Container Health Check
```
NAMES                      STATUS                    PORTS
lakehouse_hive_server2     Up 5 minutes (healthy)    0.0.0.0:10000->10000/tcp
lakehouse_hive_metastore   Up 17 minutes (healthy)   0.0.0.0:9083->9083/tcp
```

### Database Connectivity Test
```
Connected to: Apache Hive (version 4.0.0)
Driver: Hive JDBC (version 4.0.0)
Transaction isolation: TRANSACTION_REPEATABLE_READ
```

### Database Listing Test
```
+----------------+
| database_name  |
+----------------+
| bronze         |
| default        |
+----------------+
2 rows selected (1.566 seconds)
```

### Table Creation Test
```
[OK] Table operations working correctly
  Tables in 'default' database:
    - health_check_test
```

---

## JDBC Connection Details

### Connection URL Format
```
jdbc:hive2://<host>:<port>/<database>;auth=noSasl
```

### From Host Machine
```
jdbc:hive2://localhost:10000/default;auth=noSasl
```

### From Docker Network
```
jdbc:hive2://hive-server2:10000/default;auth=noSasl
```

### Required Driver
- **Group ID**: `org.apache.hive`
- **Artifact ID**: `hive-jdbc`
- **Version**: `4.0.0`

---

## Monitoring & Logs

### View HiveServer2 Logs
```bash
# Container logs
docker logs lakehouse_hive_server2 --tail 100 -f

# Application logs
docker exec lakehouse_hive_server2 tail -f /tmp/hive/hive.log
```

### View Metastore Logs
```bash
docker logs lakehouse_hive_metastore --tail 100 -f
```

### Check PostgreSQL Metadata
```bash
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db -c '\dt'
```

### Access Web UI
- **HiveServer2 Web UI**: http://localhost:10002
- **Spark Master UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001

---

## Troubleshooting Quick Reference

### Issue: Can't connect to HiveServer2
**Solution:** Ensure you use `auth=noSasl` in JDBC URL
```
jdbc:hive2://localhost:10000/default;auth=noSasl
```

### Issue: Container shows unhealthy
**Solution:** Restart the container
```bash
docker-compose restart hive-server2
```

### Issue: Port not responding
**Solution:** Check if port is bound
```bash
docker exec lakehouse_hive_server2 nc -z localhost 10000
```

### Issue: Need to rebuild
**Solution:** Rebuild and restart
```bash
docker-compose build hive-server2
docker-compose up -d hive-server2
```

---

## Storage Locations

### Data Storage (MinIO/S3)
- **Warehouse**: `s3a://bronze/warehouse`
- **Scratch**: `s3a://bronze/tmp/hive`
- **Access**: Via MinIO at `http://minio:9000`

### Local Storage (Container)
- **Scratch**: `/tmp/hive`
- **Resources**: `/tmp/hive/resources`
- **Query Logs**: `/tmp/hive/querylog`
- **Operation Logs**: `/tmp/hive/operation_logs`

### Metadata Storage (PostgreSQL)
- **Database**: `metastore_db`
- **Host**: `postgres:5432`
- **Tables**: 84 metadata tables

---

## Next Steps

### 1. Integration with Spark
Use Hive Metastore from Spark jobs:
```python
spark = SparkSession.builder \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()
```

### 2. Query from Airflow
Use PyHive or HiveOperator in Airflow DAGs

### 3. BI Tool Connection
Connect Tableau, Power BI, or Superset using JDBC driver

### 4. Data Governance
Implement metadata management and data lineage tracking

---

## Documentation

Detailed guides available:
- **Full Guide**: `docs/HIVE_DATABASE_GUIDE.md`
- **Health Check Script**: `scripts/check-hive-db.ps1`
- **Configuration**: `hive/init/hive-site.xml`
- **Docker Compose**: `docker-compose.yml`

---

## Security Notes

⚠️ **Current Configuration**: Development Mode
- Authentication: NOSASL (no authentication)
- Authorization: Disabled
- SSL/TLS: Disabled

🔒 **For Production**: Enable proper security measures
- Kerberos authentication
- SSL/TLS encryption
- Access control and authorization
- Audit logging

---

## Maintenance

### Backup Metadata
```bash
docker exec lakehouse_postgres pg_dump \
  -U lakehouse_user metastore_db > hive_metadata_backup.sql
```

### Restore Metadata
```bash
docker exec -i lakehouse_postgres psql \
  -U lakehouse_user metastore_db < hive_metadata_backup.sql
```

### Clean Logs
```bash
docker exec lakehouse_hive_server2 rm -rf /tmp/hive/*.log
```

---

## Support & Resources

### Internal Documentation
- Hive Database Guide: `docs/HIVE_DATABASE_GUIDE.md`
- Spark Guide: `docs/SPARK_GUIDE.md`
- Deployment Guide: `docs/DEPLOYMENT.md`

### External Resources
- [Apache Hive Docs](https://hive.apache.org/)
- [HiveServer2 Configuration](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients)
- [S3A FileSystem](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

---

## Summary

✅ **Hive Metastore**: Operational  
✅ **HiveServer2**: Operational  
✅ **PostgreSQL Metadata**: Operational  
✅ **JDBC Connectivity**: Working  
✅ **Table Operations**: Working  
✅ **Health Monitoring**: Script available  

**Status**: Ready for production use (with security hardening)

---

*Last Updated: October 27, 2025*  
*Verified by: Automated Health Check Script*


-- Clean Silver layer tracking logs
DELETE FROM file_ingestion_log 
WHERE layer = 'silver';

-- Show remaining records
SELECT layer, COUNT(*) as record_count 
FROM file_ingestion_log 
GROUP BY layer 
ORDER BY layer;

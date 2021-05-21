-----------------------------------------------------------------------------------------------------------
-- Create database
-----------------------------------------------------------------------------------------------------------
CREATE DATABASE ServerlessDB
    COLLATE Latin1_General_100_BIN2_UTF8  -- => recommended. This can help with performance because it will pushdown filters more efficienlty.
    --COLLATE Japanese_XJIS_100_CI_AS
    --COLLATE ​Japanese_XJIS_100_CI_AS_SC_UTF8
    --COLLATE Japanese_XJIS_140_CI_AS_UTF8
;
GO

--ALTER DATABASE ServerlessDB COLLATE Japanese_XJIS_140_CI_AS_UTF8;
--DROP DATABASE ServerlessDB2;

-----------------------------------------------------------------------------------------------------------
-- Check collation of current database
-----------------------------------------------------------------------------------------------------------
SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS Collation;

SELECT Name, Description FROM fn_helpcollations()  
    WHERE Name LIKE 'Japanese_XJIS_%'
;
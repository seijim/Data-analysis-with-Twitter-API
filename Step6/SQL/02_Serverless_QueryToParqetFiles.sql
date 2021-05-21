-- View tweets
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://your-storage-account.dfs.core.windows.net/your-filesystem/tweetdata/*/*/*/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]
;

-- Total rows
SELECT COUNT(*) total_rows FROM
    OPENROWSET(
        BULK 'https://your-storage-account.dfs.core.windows.net/your-filesystem/tweetdata/*/*/*/*/*.parquet',
        FORMAT='PARQUET'
    ) AS [result]
;

-- Count by source (device)
SELECT [Source], count(CreatedBy) AS [count] FROM (
    SELECT distinct CreatedBy, [Source]
    FROM
        OPENROWSET(
            BULK 'https://your-storage-account.dfs.core.windows.net/your-filesystem/tweetdata/*/*/*/*/*.parquet',
            FORMAT='PARQUET'
        ) AS [result]
) AS T
GROUP BY [Source]
ORDER BY [count] DESC
;
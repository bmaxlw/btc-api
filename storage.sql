-- [22.06.2022]: Target Database and related objects creation (one-batch script)
USE [master];
GO
DROP DATABASE IF EXISTS BTC;
GO
CREATE DATABASE BTC;
GO
USE [BTC];
GO

/****** Object:  Table [dbo].[BTC_rate]    Script Date: 4/21/2022 4:05:15 PM ******/
CREATE TABLE [dbo].[BTC_rate](
	[local_time] [datetime] NOT NULL,
	[utc_time] [nvarchar](50) NULL,
	[BTC_USD] [numeric](19, 2) NULL,
	[BTC_EUR] [numeric](19, 2) NULL,
	[BTC_GBP] [numeric](19, 2) NULL,
	[BTC_USD_change] [numeric](19, 7) NULL,
	[BTC_EUR_change] [numeric](19, 7) NULL,
	[BTC_GBP_change] [numeric](19, 7) NULL,
PRIMARY KEY CLUSTERED 
(
	[local_time] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[BTC_rate] ADD  DEFAULT (getdate()) FOR [local_time]
GO

CREATE TABLE Metrics
(
    record_id INT NOT NULL PRIMARY KEY IDENTITY(1, 1),
    local_time DATETIME NOT NULL,
    min_rate NUMERIC(19, 2),
    max_rate NUMERIC(19, 2),
    avg_rate NUMERIC(19, 2),
    min_change NUMERIC(19, 6),
    max_change NUMERIC(19, 6),
    avg_change NUMERIC(19, 6),
    std_usd NUMERIC(19, 6),
    std_eur NUMERIC(19, 6),
    std_gbp NUMERIC(19, 6),
    increase NUMERIC(19, 2),
    decrease NUMERIC(19, 2),
    retain NUMERIC(19, 2),
    FOREIGN KEY (local_time) REFERENCES dbo.BTC_rate (local_time)
)
GO

-- [21.04.2022]: Metrics for monitoring
CREATE OR ALTER FUNCTION fn_Metrics ()
RETURNS TABLE
AS
RETURN
(
    SELECT GETDATE() AS [time],
           ROUND(MIN(BTC_USD), 2) AS min_rate,
           ROUND(MAX(BTC_USD), 2) AS max_rate,
           ROUND(AVG(BTC_USD), 2) AS avg_rate,
           ROUND(MIN(BTC_USD_change), 6) AS min_change,
           ROUND(MAX(BTC_USD_change), 6) AS max_change,
           ROUND(AVG(BTC_USD_change), 6) AS avg_change,
           ROUND(STDEV(BTC_USD), 2) AS std_usd,
           ROUND(STDEV(BTC_EUR), 2) AS std_eur,
           ROUND(STDEV(BTC_GBP), 2) AS std_gbp,
           ROUND(
                    (
                        SELECT CAST(COUNT(BTC_USD_change) AS NUMERIC(5, 2))
                        FROM BTC_rate
                        WHERE BTC_USD_change > 0
                    ) / CAST(COUNT(*) AS NUMERIC(5, 2)),
                    2
                ) AS Rise,
           ROUND(
                    (
                        SELECT CAST(COUNT(BTC_USD_change) AS NUMERIC(5, 2))
                        FROM BTC_rate
                        WHERE BTC_USD_change < 0
                    ) / CAST(COUNT(*) AS NUMERIC(5, 2)),
                    2
                ) AS Fall,
           CASE
               WHEN
               (
                   SELECT COUNT(BTC_USD_change) FROM BTC_rate WHERE BTC_USD_change = 0
               ) = 0 THEN
                   0
               ELSE
                   ROUND(
                            (
                                SELECT CAST(COUNT(BTC_USD_change) AS NUMERIC(5, 2))
                                FROM BTC_rate
                                WHERE BTC_USD_change = 0
                            ) / CAST(COUNT(*) AS NUMERIC(5, 2)),
                            2
                        )
           END AS Retain
    FROM dbo.BTC_rate
);
GO
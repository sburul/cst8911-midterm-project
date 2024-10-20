-- Creating a database in Synapse 
CREATE DATABASE ImmigrationData;

-- Using the database 
USE ImmigrationData;

-- Creating a master key which is required for PolyBase
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'cst8915@algonquin';

-- Creating a database scoped credential using the SAS token created before
CREATE DATABASE SCOPED CREDENTIAL BlobStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
-- Secret Token
SECRET = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-10-22T16:12:32Z&st=2024-10-19T08:12:32Z&spr=https&sig=iXBC1%2BIp33qbtmwEPtS3hyLi59ili5BIMOVYaLBl2Pw%3D';  

-- Creating an external data source to connect to Blob Storage
CREATE EXTERNAL DATA SOURCE BlobStorage
WITH ( TYPE = HADOOP,
       LOCATION = 'https://cst8915storage.blob.core.windows.net',  -- Blob endpoint from your connection string
       CREDENTIAL = BlobStorageCredential );

-- Creating an external file format for CSV
CREATE EXTERNAL FILE FORMAT CsvFileFormat
WITH ( FORMAT_TYPE = DELIMITEDTEXT,
       FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2 ) );

-- Creating an external table that points to immigration data CSV
CREATE EXTERNAL TABLE ImmigrationExternalTable (
    [Country] NVARCHAR(50),
    [Continent] NVARCHAR(50),
    [Region] NVARCHAR(50),
    [DevName] NVARCHAR(50),
    [1980] INT, [1981] INT,  [1982] INT, [1983] INT, [1984] INT,
    [1985] INT, [1986] INT,  [1987] INT, [1988] INT, [1989] INT,
    [1990] INT, [1991] INT,  [1992] INT, [1993] INT, [1994] INT,
    [1995] INT, [1996] INT,  [1997] INT, [1998] INT, [1999] INT,
    [2000] INT, [2001] INT,  [2002] INT, [2003] INT, [2004] INT,
    [2005] INT, [2006] INT,  [2007] INT, [2008] INT, [2009] INT,
    [2010] INT, [2011] INT,  [2012] INT, [2013] INT, [Total] INT
)
WITH (
    LOCATION = 'canadian_immegration_data.csv',  -- Replacing with actual file name
    DATA_SOURCE = BlobStorage,
    FILE_FORMAT = CsvFileFormat
);

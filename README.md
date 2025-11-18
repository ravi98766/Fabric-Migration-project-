***Business Scenario: Enterprise Data Migration to Microsoft Fabric***
A large retail company had its operational and analytical data distributed across Azure Data Lake Storage Gen2 (ADLS) / AWS S3 repositories. The organization wanted to centralize its analytics ecosystem by migrating all source files into Microsoft Fabric for unified reporting, governance, and lakehouse-based analytics.
To achieve this, I designed and implemented an end-to-end data ingestion and deduplication framework using Fabric Data Pipelines, Lakehouse, and Warehouse.

***What I Delivered***

***1. Migrated 50+ Source Files Into Fabric***
   
•	Connected to ADLS Gen2 storage (can also work for S3).

•	Ingested files of mixed formats (CSV, Excel).

•	Loaded them into Fabric Lakehouse as Parquet optimized files.

***2. Created a Fabric Lakehouse + Warehouse Model***
   
•	Lakehouse stored curated Parquet data for analytics.

•	Warehouse contained a single Delta table for unified reporting.

•	Ensured source-to-target schema alignment and metadata consistency.

***3. Implemented a FileTracker Lookup Table***

To avoid re-loading already processed files, I built a FileTracker table with:
 Column  	Description
FileName--	Name of file processed
LoadDate--	When it was ingested

<img width="768" height="368" alt="Screenshot (1031)" src="https://github.com/user-attachments/assets/88379e7d-83d2-4d55-8dfe-3b30d1f0d45a" />


***This FileTracker:***

•	Detects new files arriving in ADLS.
•	Identifies updated files for reprocessing.
•	Prevents duplicate ingestion into Lakehouse & Warehouse.
•	Drives the logic in Filter + ForEach activities



***4. Designed a Reusable Fabric Data Pipeline***

CREATE TABLE DimProduct (
    ProductID INT,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price FLOAT,
    FileName VARCHAR(100),
    LoadDate DATETIME2(0)
);


CREATE TABLE FileTracker (
    FileName VARCHAR(100) ,
    LoadDate DATETIME2(0)
);

select * from FileTracker
select * from DimProduct

***TO TEST THE PIPELINE I INSERTED 1 RECORD  MANUALLY-***

insert into FileTracker values('product_2024-07-01.csv','')
 
The pipeline included:

<img width="1121" height="279" alt="data pipeline" src="https://github.com/user-attachments/assets/171d7db4-84ab-4c13-b091-a955b3c50680" />

🔹 Lookup Activity

Reads FileTracker and fetches the list of previously loaded files.

🔹 Get Metadata / List Files
Fetches all current files in ADLS.

🔹 Filter Activity

Filters only new or updated files:

@not(contains(string(activity('Lookup_FileTracker').output.value),
              item().name))
              
🔹 ForEach Activity

Executes exactly once per unique new file:

•	Copy data to Lakehouse (Parquet)

•	Load into Warehouse Delta Table

•	Append entry into FileTracker

***5. Ensured Zero Duplicacy-script Activity***
•	Warehouse ingestion used MERGE INTO with business keys.

•	Lakehouse stored only unique Parquet file versions.

•	FileTracker enforced strict change detection.

***script activity–*** 

INSERT INTO FileTracker (FileName, LoadDate)
VALUES ('@{item().name}', GETUTCDATE());

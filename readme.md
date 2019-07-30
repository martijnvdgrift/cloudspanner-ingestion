# Google Cloud Spanner ingestion

Small project which ingests some Cloud Spanner data into a db table with schema:
```sql
CREATE TABLE Songs (
   SongId  STRING(36) NOT NULL,
   Title   STRING(100),
   year    INT64,
   peak    INT64,
) PRIMARY KEY(SongId);
```

## Usage
- Clone project into a Cloud Shell (to use it's embedded authentication)
- Run the application with `mvn exec:java` 



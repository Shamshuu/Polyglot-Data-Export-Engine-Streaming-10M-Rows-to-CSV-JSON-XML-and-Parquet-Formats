# Polyglot Data Export Engine (Streaming 10M Rows)

A high-performance, memory-efficient data export engine built with Java 21, Spring Boot, and PostgreSQL. It streams large datasets (up to 10,000,000+ rows) into multiple formats (CSV, JSON, XML, Parquet) under a strict container memory limit of **256MB**.

## Architecture & Design Decisions

### 1. Database Cursor-Based Streaming (JDBC Fetch Size)
Loading 10 million rows into application memory would lead to instant Out of Memory (OOM) crashes. To solve this:
*   We use PostgreSQL server-side cursors. In JDBC, this is enabled by disabling auto-commit (`connection.setAutoCommit(false)`) and setting a positive fetch size (`statement.setFetchSize(10000)`).
*   The application fetches rows from the PostgreSQL database in chunks of 10,000, serializes them on-the-fly, and pushes them directly to the client HTTP response stream.

### 2. Format-Specific Streaming Serializers
*   **CSV**: Writes rows on-the-fly, escaping values (double quotes, commas, newlines) and serializing the nested `metadata` JSONB as a string.
*   **JSON**: Streams a single JSON array of objects (`[{...}, {...}]`) element-by-element, embedding the `metadata` JSONB column directly as a native sub-object without parsing overhead.
*   **XML**: Output is generated recursively, translating the nested JSONB `metadata` structure into nested XML tags (e.g. `<metadata><sku>SKU-1</sku><details><weight>1.5</weight></details></metadata>`).
*   **Parquet**: Since Parquet is a columnar format requiring a seekable footer at the end, it cannot be streamed directly to a non-seekable socket. The engine writes Parquet chunks to a temporary file on disk using `ExampleParquetWriter` and then streams the file to the client in 64KB chunks.

### 3. Strict Memory Capping (256MB limit)
To run reliably under a 256MB container memory limit:
*   **JVM Tuning**: The JVM is configured with `-Xmx128m -Xms128m -XX:MaxMetaspaceSize=64m -XX:+UseG1GC`. This limits heap to 128MB, leaving enough memory for metaspace, threads, and container OS processes.
*   **Parquet Row Group Optimization**: The default Parquet row group size is 128MB. To prevent OOM errors, the engine overrides the row group size to **1MB** and page size to **64KB**, ensuring the Parquet writer flushes to disk frequently and maintains a tiny memory footprint.

---

## Technical Stack
*   **Java 21**
*   **Spring Boot 3.3.5 (Spring Web)**
*   **PostgreSQL JDBC Driver**
*   **Apache Parquet & Hadoop Common**
*   **Docker & Docker Compose**

---

## API Documentation

### 1. Initiate Export Job
Creates an export configuration and returns a unique Job ID.

*   **Endpoint**: `POST /exports`
*   **Request Schema**:
    ```json
    {
      "format": "csv | json | xml | parquet",
      "columns": [
        { "source": "id", "target": "id" },
        { "source": "name", "target": "name" },
        { "source": "value", "target": "value" },
        { "source": "metadata", "target": "metadata" }
      ],
      "compression": "gzip"
    }
    ```
    *Note: `compression: "gzip"` is optional and only supported for text formats (`csv`, `json`, `xml`).*
*   **Response Schema (201 Created)**:
    ```json
    {
      "exportId": "a90df265-5b43-4cb5-827d-080c551ad36e",
      "status": "pending"
    }
    ```

### 2. Download Export Stream
Streams the exported dataset for a given Job ID.

*   **Endpoint**: `GET /exports/{exportId}/download`
*   **Response Headers**:
    *   **CSV**: `Content-Type: text/csv`, `Content-Disposition: attachment; filename="export.csv"`
    *   **JSON**: `Content-Type: application/json`, `Content-Disposition: attachment; filename="export.json"`
    *   **XML**: `Content-Type: application/xml`, `Content-Disposition: attachment; filename="export.xml"`
    *   **Parquet**: `Content-Type: application/octet-stream`, `Content-Disposition: attachment; filename="export.parquet"`
    *   **Compression**: If `gzip` was specified, `Content-Encoding: gzip` is added and the body is compressed.

### 3. Run Performance Benchmark
Triggers a full export of all 10 million rows across all 4 formats, records key metrics, and returns the results.

*   **Endpoint**: `GET /exports/benchmark`
*   **Response Schema (200 OK)**:
    ```json
    {
      "datasetRowCount": 10000000,
      "results": [
        {
          "format": "csv",
          "durationSeconds": 30.5,
          "fileSizeBytes": 894562124,
          "peakMemoryMB": 135.2
        },
        ...
      ]
    }
    ```

---

## Security Features
1.  **SQL Injection Prevention**: Source columns are strictly validated against a hardcoded whitelist (`id`, `created_at`, `name`, `value`, `metadata`). Dynamic SQL strings are built only after whitelist validation.
2.  **Schema Conformance**: Target column names are validated against the regex `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent formatting issues (e.g. invalid XML tags) and code injection.
3.  **Job ID Safety**: Export jobs are referenced by cryptographically secure UUIDs.

---

## Getting Started (One-Command Setup)

### Prerequisites
*   Docker and Docker Compose installed and running.

### Launching the Application
Run the following command in the repository root:
```bash
docker-compose up --build
```

This single command will:
1.  Launch the PostgreSQL container.
2.  Run the idempotent seeding script `init-db.sh` to generate exactly 10,000,000 records (with nested JSONB).
3.  Launch the Spring Boot application container once the database healthcheck reports healthy.
4.  Expose the application on port `8080`.

### Verifying Seeding
To verify the database has exactly 10,000,000 rows:
```bash
docker-compose exec db psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
```
Output:
```
  count   
----------
 10000000
(1 row)
```
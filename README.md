# ClickHouse <-> Flat File Data Ingestor

A simple web application to facilitate bidirectional data ingestion between a ClickHouse database and flat files (CSV/TSV), with support for JWT authentication and column selection.

## Features

* **Bidirectional Flow:**
    * ClickHouse table -> Flat File (CSV)
    * Flat File (CSV/TSV upload) -> ClickHouse table (will attempt `CREATE TABLE IF NOT EXISTS` based on CSV header)
* **Source/Target Selection:** Choose data direction via UI radio buttons.
* **ClickHouse Configuration:** Input Host, Port, Database, User (optional), and JWT Token/Password.
* **JWT Authentication:** Connects to ClickHouse using the provided JWT token via the `clickhouse-connect` library. Basic user/password is also supported if token is omitted but user is provided.
* **Flat File Configuration:** Upload local files, specify delimiter (Comma, Tab, Semicolon, Pipe, Space), and optionally name output files.
* **Schema Discovery & Column Selection:**
    * Fetches table list from ClickHouse.
    * Fetches column names from selected ClickHouse table or uploaded Flat File header.
    * Allows selection of specific columns for ingestion using checkboxes.
* **Data Preview:** Preview the first 100 rows of the selected source data (with selected columns) before starting full ingestion.
* **Efficient Handling:** Uses Pandas DataFrames and chunking (for Flat File -> ClickHouse) for better memory management.
* **Completion Reporting:** Displays the total number of records processed upon successful ingestion.
* **Download Link:** Provides a download link for files generated from ClickHouse.
* **Basic Error Handling:** Displays user-friendly messages for connection, authentication, query, file, and ingestion errors.

## Technology Stack

* **Backend:** Python 3.x, Flask
* **ClickHouse Client:** `clickhouse-connect`
* **Data Handling:** Pandas
* **Frontend:** HTML, CSS, Vanilla JavaScript (Fetch API)

## Setup

1.  **Prerequisites:**
    * Python 3.7+ and `pip` installed.
    * Access to a ClickHouse instance (local Docker or cloud).
    * Sample data loaded into ClickHouse (e.g., `uk_price_paid`, `ontime` from [ClickHouse Docs](https://clickhouse.com/docs/en/getting-started/example-datasets)).
    * Sample CSV/TSV files for testing the Flat File -> ClickHouse direction.

2.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd clickhouse_flatfile_ingestor
    ```

3.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Create Necessary Folders:**
    Ensure the `uploads` and `downloads` directories exist in the project root:
    ```bash
    mkdir uploads
    mkdir downloads
    ```
    *(Note: The `app.py` script also attempts to create these if they don't exist)*

## Running the Application

1.  **Start the Flask Development Server:**
    ```bash
    flask run --host=0.0.0.0 --port=5000
    ```
    *(Using `--host=0.0.0.0` makes it accessible from your local network)*

2.  **Access the Application:**
    Open your web browser and navigate to `http://localhost:5000` (or your machine's IP address if accessing from another device on the network).

## Usage Guide

1.  **Select Source & Target:** Choose whether ClickHouse or Flat File is the source and target using the radio buttons. The configuration sections will adjust automatically.
2.  **Configure ClickHouse (if applicable):**
    * Enter the Host, Port (e.g., 8123 for HTTP, 8443 for HTTPS), Database name.
    * *Authentication:*
        * **JWT:** Leave User blank and enter the JWT token in the "JWT Token (or Password)" field.
        * **User/Password:** Enter the User and Password (in the "JWT Token (or Password)" field).
    * Click "Test Connection".
    * If successful and ClickHouse is the source, click "Load Tables" and select a table from the dropdown.
    * If ClickHouse is the target, enter the desired "Target Table Name". The application will try to create it if it doesn't exist based on the source CSV schema (use with caution, manual creation might be safer for complex schemas).
3.  **Configure Flat File (if applicable):**
    * **Source:** Click the "Choose File" button, select your CSV/TSV file. Wait for the "Uploaded: filename" status.
    * **Target:** Optionally enter a base name for the output file (a unique ID and `.csv` will be appended).
    * Select the correct "Delimiter" for your file (source or target).
4.  **Load Columns:** Once the source (CH table or uploaded file) is configured, click "Load Columns from Source".
5.  **Select Columns:** Check the boxes next to the columns you want to include in the ingestion. Use "Select All" / "Deselect All" for convenience.
6.  **(Optional) Preview Data:** Click "Preview Data" to see the first 100 rows of the selected source columns in a table below the form.
7.  **Start Ingestion:** Click "Start Ingestion".
8.  **Monitor Status:** Observe the "Status" area for progress updates (Connecting, Loading, Ingesting, Completed, Error).
9.  **View Results:**
    * On success, a message indicating the number of records processed will appear.
    * If the target was a Flat File, a download link for the generated CSV file will appear.
    * On failure, an error message will be displayed.

## Testing

* **Test Case 1 (CH -> FF):**
    * Source: ClickHouse, Target: Flat File
    * Select a table (e.g., `uk_price_paid`). Load columns. Select a few columns.
    * Start Ingestion. Verify the success message count matches expectations. Download the file and check its content and structure.
* **Test Case 2 (FF -> CH):**
    * Source: Flat File, Target: ClickHouse
    * Prepare a sample CSV file. Upload it. Select the correct delimiter.
    * Enter a *new* Target Table Name in the ClickHouse config section.
    * Load columns. Select columns.
    * Start Ingestion. Verify success message and record count.
    * Connect to ClickHouse separately (e.g., via `clickhouse-client`) and query the new table (`SELECT * FROM your_new_table LIMIT 10;`, `SELECT count() FROM your_new_table;`) to verify data and count.
* **Test Case 3 (Connection/Auth Failures):**
    * Enter incorrect Host/Port/Token/Password details for ClickHouse.
    * Click "Test Connection" or attempt to Load Tables/Ingest. Verify appropriate error messages are shown.
* **Test Case 4 (Preview):**
    * Configure a source (CH or FF). Load and select columns.
    * Click "Preview Data". Verify a table with the first 100 rows and selected columns appears correctly. Test with both CH and FF sources.

## Limitations & Considerations

* **Error Handling:** Basic error handling is implemented. More granular error reporting could be added.
* **Data Type Mapping (FF -> CH):** The automatic table creation attempts to infer ClickHouse types from Pandas types read from the CSV. This might not always be accurate, especially for complex types (Nested, LowCardinality, Nullable, Dates/Times). For critical applications, ensure the target table exists with the correct schema *before* ingestion, or enhance the type mapping logic. Data type mismatches during insertion can cause errors.
* **Large Files:** While chunking is used for FF->CH, extremely large file uploads/downloads might strain server memory or browser resources. Streaming approaches would be more robust for massive datasets.
* **ClickHouse JOINs (Bonus):** This implementation does not include the bonus requirement for joining multiple ClickHouse tables. This would require significant UI additions (selecting multiple tables, defining join keys/types) and backend logic to construct the JOIN query.
* **Security:** This is a basic implementation. For production, consider:
    * More robust input validation and sanitization (especially for table/column names).
    * Authentication/Authorization for the web app itself (if needed beyond ClickHouse auth).
    * Rate limiting.
    * Secure handling of secrets (e.g., using environment variables or a proper secrets management system instead of direct UI input for production tokens/passwords).
* **Scalability:** Designed for single-user interaction on a single server. Scaling would require different architecture (task queues, multiple workers, etc.).

## AI Prompts Used

*(See `prompts.txt` file)*

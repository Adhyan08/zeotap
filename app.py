import os
import uuid
import pandas as pd
from flask import Flask, request, jsonify, render_template, send_from_directory
import clickhouse_connect
from werkzeug.utils import secure_filename

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
DOWNLOAD_FOLDER = 'downloads'
ALLOWED_EXTENSIONS = {'csv', 'tsv', 'txt'} # Allow common flat file extensions

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.secret_key = os.urandom(24) # Needed for flash messages or session, good practice

# Create upload/download directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# --- Helper Functions ---

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_clickhouse_client(config):
    """Establishes connection to ClickHouse."""
    try:
        auth_params = {}
        if config.get('token'):
            # Use JWT token if provided
            auth_params['jwt'] = config['token']
        elif config.get('user'):
             # Basic user/password auth (add password field to UI if needed)
            auth_params['user'] = config['user']
            auth_params['password'] = config.get('password', '') # Assuming password might be needed

        client = clickhouse_connect.get_client(
            host=config['host'],
            port=int(config['port']),
            database=config['database'],
            **auth_params,
            # secure=True # Uncomment if using https/secure connection (ports 8443, 9440)
        )
        client.ping() # Verify connection
        print("ClickHouse connection successful.")
        return client
    except Exception as e:
        print(f"ClickHouse connection error: {e}")
        raise ConnectionError(f"Failed to connect to ClickHouse: {e}")

def get_flatfile_delimiter(delimiter_name):
    """Maps common delimiter names to characters."""
    delimiters = {
        "comma": ",",
        "tab": "\t",
        "semicolon": ";",
        "pipe": "|",
        "space": " "
    }
    return delimiters.get(delimiter_name.lower(), delimiter_name) # Return character or original string if not found

# --- Flask Routes ---

@app.route('/')
def index():
    """Renders the main UI page."""
    return render_template('index.html')

@app.route('/connect_clickhouse', methods=['POST'])
def connect_clickhouse():
    """Attempts to connect to ClickHouse and returns status."""
    config = request.json
    try:
        client = get_clickhouse_client(config)
        client.close() # Close connection after ping test
        return jsonify({"success": True, "message": "Connection successful!"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@app.route('/get_tables', methods=['POST'])
def get_tables():
    """Fetches table names from the specified ClickHouse database."""
    config = request.json
    try:
        client = get_clickhouse_client(config)
        # Use system.tables to get tables in the specified database
        query = f"SELECT name FROM system.tables WHERE database = '{config['database']}' ORDER BY name"
        tables = client.query_df(query)['name'].tolist()
        client.close()
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": f"Failed to fetch tables: {e}"}), 500

@app.route('/get_columns', methods=['POST'])
def get_columns():
    """Fetches column names from ClickHouse table or Flat File header."""
    data = request.json
    source_type = data.get('sourceType')

    try:
        if source_type == 'clickhouse':
            ch_config = data.get('clickhouseConfig')
            table_name = data.get('tableName')
            if not table_name:
                 return jsonify({"error": "Table name is required for ClickHouse source."}), 400

            client = get_clickhouse_client(ch_config)
            # Use DESCRIBE TABLE to get column names and types
            # Be careful with quoting table names if they contain special chars
            query = f"DESCRIBE TABLE \"{ch_config['database']}\".\"{table_name}\""
            describe_df = client.query_df(query)
            columns = describe_df['name'].tolist() # Get just the column names
            client.close()
            return jsonify({"columns": columns})

        elif source_type == 'flatfile':
            filename = data.get('filename')
            delimiter_name = data.get('delimiter', 'comma')
            delimiter = get_flatfile_delimiter(delimiter_name)

            if not filename:
                 return jsonify({"error": "Filename is required for Flat File source."}), 400

            filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(filename))
            if not os.path.exists(filepath):
                 return jsonify({"error": f"File not found: {filename}. Please upload it first."}), 404

            # Read only the header row using pandas
            header_df = pd.read_csv(filepath, sep=delimiter, nrows=0)
            columns = header_df.columns.tolist()
            return jsonify({"columns": columns})
        else:
            return jsonify({"error": "Invalid source type specified."}), 400

    except FileNotFoundError:
         return jsonify({"error": f"File '{filename}' not found in uploads folder."}), 404
    except pd.errors.EmptyDataError:
        return jsonify({"error": f"File '{filename}' appears to be empty or has no header."}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to get columns: {e}"}), 500


@app.route('/upload_flatfile', methods=['POST'])
def upload_flatfile():
    """Handles flat file uploads."""
    if 'flatFile' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['flatFile']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        try:
            file.save(filepath)
            return jsonify({"success": True, "filename": filename, "message": f"File '{filename}' uploaded successfully."})
        except Exception as e:
             return jsonify({"error": f"Failed to save file: {e}"}), 500
    else:
        return jsonify({"error": f"File type not allowed. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}"}), 400

@app.route('/preview_data', methods=['POST'])
def preview_data():
    """Fetches the first N records for preview."""
    data = request.json
    source_type = data.get('sourceType')
    selected_columns = data.get('columns', [])
    limit = 100 # Number of records for preview

    if not selected_columns:
        return jsonify({"error": "Please select at least one column for preview."}), 400

    try:
        if source_type == 'clickhouse':
            ch_config = data.get('clickhouseConfig')
            table_name = data.get('tableName')
            client = get_clickhouse_client(ch_config)

            # Quote column names properly
            quoted_columns = [f'"{col}"' for col in selected_columns]
            cols_str = ', '.join(quoted_columns)
            # Quote table name properly
            query = f'SELECT {cols_str} FROM "{ch_config["database"]}"."{table_name}" LIMIT {limit}'

            preview_df = client.query_df(query)
            client.close()
             # Convert DataFrame to list of dictionaries for JSON compatibility
            preview_data = preview_df.to_dict(orient='records')
            return jsonify({"previewData": preview_data, "columns": selected_columns})

        elif source_type == 'flatfile':
            filename = data.get('filename')
            delimiter_name = data.get('delimiter', 'comma')
            delimiter = get_flatfile_delimiter(delimiter_name)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(filename))

            if not os.path.exists(filepath):
                 return jsonify({"error": f"File not found: {filename}"}), 404

            # Read preview data using pandas
            preview_df = pd.read_csv(filepath, sep=delimiter, usecols=selected_columns, nrows=limit)
            # Convert DataFrame to list of dictionaries
            preview_data = preview_df.to_dict(orient='records')
            return jsonify({"previewData": preview_data, "columns": selected_columns})

        else:
             return jsonify({"error": "Invalid source type for preview."}), 400

    except FileNotFoundError:
         return jsonify({"error": f"File '{filename}' not found."}), 404
    except pd.errors.EmptyDataError:
        return jsonify({"error": f"File '{filename}' appears to be empty."}), 400
    except KeyError as e:
         return jsonify({"error": f"Column not found in source: {e}. Check selected columns."}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to preview data: {e}"}), 500


@app.route('/start_ingestion', methods=['POST'])
def start_ingestion():
    """Handles the main data ingestion logic."""
    data = request.json
    source_type = data.get('sourceType')
    target_type = data.get('targetType')
    selected_columns = data.get('columns', [])

    if not selected_columns:
        return jsonify({"error": "Please select columns to ingest."}), 400

    total_records = 0

    try:
        # --- Flow: ClickHouse -> Flat File ---
        if source_type == 'clickhouse' and target_type == 'flatfile':
            ch_config = data.get('clickhouseConfig')
            table_name = data.get('tableName')
            ff_config = data.get('flatfileConfig')
            delimiter = get_flatfile_delimiter(ff_config.get('delimiter', 'comma'))
            output_filename_base = ff_config.get('filename') or f"{table_name}_export"
            output_filename = secure_filename(f"{output_filename_base}_{uuid.uuid4()}.csv") # Add UUID to avoid overwrites
            output_filepath = os.path.join(app.config['DOWNLOAD_FOLDER'], output_filename)

            client = get_clickhouse_client(ch_config)

            # Quote column names
            quoted_columns = [f'"{col}"' for col in selected_columns]
            cols_str = ', '.join(quoted_columns)
             # Quote table name
            query = f'SELECT {cols_str} FROM "{ch_config["database"]}"."{table_name}"'

            print(f"Executing query: {query}")
            # Stream data using query_df for potentially large tables
            # For very large data, consider client.query_stream and writing row by row
            result_df = client.query_df(query)
            client.close()

            print(f"Writing {len(result_df)} records to {output_filepath}")
            # Write DataFrame to CSV
            result_df.to_csv(output_filepath, sep=delimiter, index=False, columns=selected_columns) # Ensure only selected columns are written in order

            total_records = len(result_df)
            return jsonify({
                "success": True,
                "message": f"Successfully ingested {total_records} records from ClickHouse table '{table_name}' to Flat File.",
                "filename": output_filename, # Provide filename for download link
                "recordCount": total_records
            })

        # --- Flow: Flat File -> ClickHouse ---
        elif source_type == 'flatfile' and target_type == 'clickhouse':
            ff_config = data.get('flatfileConfig')
            ch_config = data.get('clickhouseConfig')
            source_filename = secure_filename(ff_config.get('filename'))
            target_table_name = data.get('targetTableName') # Get target table name from request
            delimiter = get_flatfile_delimiter(ff_config.get('delimiter', 'comma'))
            source_filepath = os.path.join(app.config['UPLOAD_FOLDER'], source_filename)

            if not target_table_name:
                return jsonify({"error": "Target ClickHouse table name is required."}), 400
            if not os.path.exists(source_filepath):
                return jsonify({"error": f"Source file '{source_filename}' not found. Please upload first."}), 404

            client = get_clickhouse_client(ch_config)

            # --- Simple CREATE TABLE logic (Optional - Assumes table might not exist) ---
            # This is basic, production might need explicit schema definition
            # We infer types from Pandas, which can be inaccurate.
            try:
                print(f"Attempting to read schema from {source_filepath} for table creation")
                temp_df_schema = pd.read_csv(source_filepath, sep=delimiter, usecols=selected_columns, nrows=5) # Read a few rows to infer types
                col_types = pd.io.sql.get_sqltype(temp_df_schema.dtypes.to_dict(), 'clickhouse') # Try to map pandas dtypes to CH types

                # Basic type mapping corrections (adjust as needed)
                ch_col_defs = []
                for col, dtype in col_types.items():
                    if 'object' in str(temp_df_schema[col].dtype).lower() or 'string' in str(temp_df_schema[col].dtype).lower():
                        dtype = 'String' # Default object/string to String
                    elif 'datetime' in str(temp_df_schema[col].dtype).lower():
                        dtype = 'DateTime' # Map pandas datetime to DateTime
                    elif 'int' in str(temp_df_schema[col].dtype).lower():
                        dtype = 'Int64' # Default int to Int64
                    elif 'float' in str(temp_df_schema[col].dtype).lower():
                        dtype = 'Float64' # Default float to Float64
                    # Add more explicit mappings if needed
                    # Quote column names for safety
                    ch_col_defs.append(f'"{col}" {dtype}')

                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS "{ch_config['database']}"."{target_table_name}" (
                    {', '.join(ch_col_defs)}
                ) ENGINE = MergeTree() ORDER BY tuple() -- Adjust Engine and ORDER BY as needed
                """
                print(f"Executing CREATE TABLE IF NOT EXISTS statement:\n{create_table_sql}")
                client.command(create_table_sql)
                print(f"Table '{target_table_name}' checked/created.")

            except Exception as e:
                 print(f"Warning: Could not automatically create/verify table '{target_table_name}'. Assuming it exists. Error: {e}")
                 # Proceed assuming the table exists and matches the CSV structure

            # --- Ingest data in chunks ---
            chunksize = 10000  # Process N rows at a time
            print(f"Starting ingestion from '{source_filename}' to '{target_table_name}'...")
            total_records = 0
            try:
                for chunk_df in pd.read_csv(source_filepath, sep=delimiter, usecols=selected_columns, chunksize=chunksize, keep_default_na=False, na_values=['']):
                    # Important: Ensure column order matches ClickHouse table if reading specific cols
                    # Reorder DataFrame columns to match the selection order if necessary
                    chunk_df = chunk_df[selected_columns]

                    # Convert potential Pandas specific types (like NaN) to None for ClickHouse
                    # `insert_df` handles some conversions, but being explicit can help
                    # chunk_df = chunk_df.astype(object).where(pd.notnull(chunk_df), None) # Convert NaN to None

                    print(f"Inserting chunk of {len(chunk_df)} records...")
                    # Use insert_df for convenience (handles type conversions better than basic insert)
                    client.insert_df(f'"{ch_config["database"]}"."{target_table_name}"', chunk_df)
                    # OR use client.insert:
                    # data_to_insert = chunk_df.to_dict('records')
                    # client.insert(f'"{ch_config["database"]}"."{target_table_name}"', data_to_insert, column_names=selected_columns)

                    total_records += len(chunk_df)
                    print(f"Total records processed so far: {total_records}")

                client.close()
                print("Ingestion complete.")
                return jsonify({
                    "success": True,
                    "message": f"Successfully ingested {total_records} records from '{source_filename}' into ClickHouse table '{target_table_name}'.",
                    "recordCount": total_records
                })

            except Exception as insert_err:
                 client.close()
                 print(f"Error during chunk insertion: {insert_err}")
                 # Attempt to provide more specific error feedback
                 if "Unknown identifier" in str(insert_err) or "Cannot parse input" in str(insert_err):
                     return jsonify({"error": f"Ingestion failed. Possible column mismatch or data type issue between CSV and target table '{target_table_name}'. Check table schema and CSV data. Error: {insert_err}"}), 500
                 else:
                    return jsonify({"error": f"Ingestion failed after processing some chunks: {insert_err}"}), 500

        else:
            return jsonify({"error": "Invalid source/target combination selected."}), 400

    except FileNotFoundError as e:
         return jsonify({"error": f"File not found: {e}"}), 404
    except pd.errors.ParserError as e:
        return jsonify({"error": f"Error parsing CSV file. Check delimiter and file format. Error: {e}"}), 400
    except KeyError as e:
        # Handle cases where a selected column doesn't exist in the source during processing
        return jsonify({"error": f"Selected column '{e}' not found in the source data."}), 400
    except clickhouse_connect.driver.exceptions.DatabaseError as e:
         # More specific ClickHouse errors during query/insert
         return jsonify({"error": f"ClickHouse database error: {e}"}), 500
    except ConnectionError as e:
         # Catch connection errors raised by get_clickhouse_client
         return jsonify({"error": str(e)}), 500
    except Exception as e:
        # Catch-all for other unexpected errors
        import traceback
        traceback.print_exc() # Log the full traceback to the console
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


@app.route('/download/<filename>')
def download_file(filename):
    """Provides the generated flat file for download."""
    safe_filename = secure_filename(filename) # Ensure filename is safe
    directory = app.config['DOWNLOAD_FOLDER']
    try:
        return send_from_directory(directory, safe_filename, as_attachment=True)
    except FileNotFoundError:
         return jsonify({"error": "File not found or access denied."}), 404


if __name__ == '__main__':
    # Make sure debug=False for production
    app.run(debug=True, host='0.0.0.0', port=8000) # Run on port 5000, accessible externally
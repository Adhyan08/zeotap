document.addEventListener('DOMContentLoaded', () => {
    // --- DOM Elements ---
    const form = document.getElementById('ingestion-form');
    const statusArea = document.getElementById('status-area');
    const resultArea = document.getElementById('result-area');
    const errorArea = document.getElementById('error-area');
    const downloadArea = document.getElementById('download-area');
    const previewArea = document.getElementById('preview-section');
    const previewTableContainer = document.getElementById('preview-table-container');

    const btnConnectCh = document.getElementById('btn-connect-ch');
    const btnLoadTables = document.getElementById('btn-load-tables');
    const btnLoadColumns = document.getElementById('btn-load-columns');
    const btnSelectAll = document.getElementById('btn-select-all-columns');
    const btnDeselectAll = document.getElementById('btn-deselect-all-columns');
    const btnPreview = document.getElementById('btn-preview');
    const btnStartIngestion = document.getElementById('btn-start-ingestion');

    const chTableSelect = document.getElementById('ch-table-select');
    const columnListDiv = document.getElementById('column-list');
    const fileUploadInput = document.getElementById('flatfile-upload');
    const uploadStatusSpan = document.getElementById('upload-status');
    const uploadedFilenameInput = document.getElementById('uploaded-filename');

    // --- Helper Functions ---
    function updateStatus(message, isError = false, isSuccess = false) {
        statusArea.textContent = `Status: ${message}`;
        statusArea.className = isError ? 'status error' : (isSuccess ? 'status success' : 'status');
        if (isError) {
             errorArea.textContent = message;
             errorArea.style.display = 'block';
             resultArea.style.display = 'none';
             downloadArea.innerHTML = ''; // Clear download link on new error
        } else {
             errorArea.textContent = '';
             errorArea.style.display = 'none';
        }
         if(!isError && !isSuccess) {
            resultArea.style.display = 'none'; // Hide result unless success
            downloadArea.innerHTML = ''; // Clear download link
        }
         // Disable buttons during processing
         const buttons = form.querySelectorAll('button');
         buttons.forEach(btn => btn.disabled = (message.includes('Connecting') || message.includes('Loading') || message.includes('Ingesting') || message.includes('Uploading')));
    }

    function displayResult(message, filename = null, recordCount = null) {
        resultArea.textContent = message;
        resultArea.style.display = 'block';
        statusArea.className = 'status success'; // Set status style to success
        statusArea.textContent = `Status: Completed`; // Update main status too

        // Enable buttons after completion
        enableActionButtons();


        if (filename) {
            downloadArea.innerHTML = `<a href="/download/${filename}" target="_blank">Download ${filename}</a>`;
        } else {
            downloadArea.innerHTML = '';
        }
    }

     function getClickHouseConfig() {
        return {
            host: document.getElementById('ch-host').value,
            port: document.getElementById('ch-port').value,
            database: document.getElementById('ch-db').value,
            user: document.getElementById('ch-user').value,
            token: document.getElementById('ch-token').value, // Could be JWT or password
        };
    }

     function getFlatFileConfig() {
        return {
            filename: uploadedFilenameInput.value, // Use the stored filename after upload
            delimiter: document.getElementById('ff-delimiter').value,
            outputFilename: document.getElementById('ff-output-name').value
        };
    }

    function enableActionButtons() {
        // Enable buttons based on current state
        const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
        const targetType = document.querySelector('input[name="targetType"]:checked').value;
        const columnsSelected = columnListDiv.querySelectorAll('input[type="checkbox"]:checked').length > 0;

        btnLoadTables.disabled = !getClickHouseConfig().host || !getClickHouseConfig().port || !getClickHouseConfig().database; // Requires CH config

        let canLoadColumns = false;
         if (sourceType === 'clickhouse' && chTableSelect.value) {
            canLoadColumns = true;
        } else if (sourceType === 'flatfile' && uploadedFilenameInput.value) {
            canLoadColumns = true;
        }
         btnLoadColumns.disabled = !canLoadColumns;

         btnPreview.disabled = !columnsSelected;
         btnStartIngestion.disabled = !columnsSelected;
         btnSelectAll.disabled = columnListDiv.querySelectorAll('input[type="checkbox"]').length === 0;
         btnDeselectAll.disabled = columnListDiv.querySelectorAll('input[type="checkbox"]').length === 0;

         // Re-enable connect button
         btnConnectCh.disabled = false;
         // Re-enable upload button
         fileUploadInput.disabled = false;
    }

     function toggleSections() {
         const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
         const targetType = document.querySelector('input[name="targetType"]:checked').value;

         const chConfigSection = document.getElementById('clickhouse-config-section');
         const ffConfigSection = document.getElementById('flatfile-config-section');

         // Show/hide based on if CH/FF is source OR target
         chConfigSection.style.display = (sourceType === 'clickhouse' || targetType === 'clickhouse') ? 'block' : 'none';
         ffConfigSection.style.display = (sourceType === 'flatfile' || targetType === 'flatfile') ? 'block' : 'none';

         // Show/hide specific elements within sections
         document.querySelectorAll('.source-ch-only').forEach(el => el.style.display = (sourceType === 'clickhouse') ? 'inline-block' : 'none'); // Use inline-block for labels/selects
         document.querySelectorAll('.target-ch-only').forEach(el => el.style.display = (targetType === 'clickhouse') ? 'inline-block' : 'none');
         document.querySelectorAll('.source-ff-only').forEach(el => el.style.display = (sourceType === 'flatfile') ? 'block' : 'none'); // Use block for divs
         document.querySelectorAll('.target-ff-only').forEach(el => el.style.display = (targetType === 'flatfile') ? 'block' : 'none');

         // Reset inputs/selections when source/target changes significantly
         columnListDiv.innerHTML = '<p>Load columns after configuring the source.</p>';
         chTableSelect.innerHTML = '<option value="">-- Load Tables First --</option>';
         chTableSelect.disabled = true;
         uploadedFilenameInput.value = '';
         uploadStatusSpan.textContent = '';
         previewArea.style.display = 'none'; // Hide preview
         resultArea.textContent = ''; // Clear results
         errorArea.textContent = ''; // Clear errors
         downloadArea.innerHTML = ''; // Clear download links

         enableActionButtons(); // Re-evaluate button states
     }


    // --- Event Listeners ---

    // Toggle sections on initial load
    toggleSections();

    // Test ClickHouse Connection
    btnConnectCh.addEventListener('click', async () => {
        updateStatus('Connecting to ClickHouse...');
        const config = getClickHouseConfig();
        try {
            const response = await fetch('/connect_clickhouse', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
            const data = await response.json();
            if (response.ok && data.success) {
                updateStatus('Connection successful!', false, true);
                 // Only enable Load Tables if CH is a source or target
                 const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
                 const targetType = document.querySelector('input[name="targetType"]:checked').value;
                 btnLoadTables.disabled = !(sourceType === 'clickhouse' || targetType === 'clickhouse');
            } else {
                updateStatus(`Connection failed: ${data.error || 'Unknown error'}`, true);
                 btnLoadTables.disabled = true;
            }
        } catch (error) {
            updateStatus(`Connection error: ${error}`, true);
             btnLoadTables.disabled = true;
        }
         enableActionButtons(); // Re-enable buttons
    });

    // Load ClickHouse Tables
    btnLoadTables.addEventListener('click', async () => {
        updateStatus('Loading tables...');
        const config = getClickHouseConfig();
        try {
            const response = await fetch('/get_tables', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
            const data = await response.json();
            if (response.ok) {
                chTableSelect.innerHTML = '<option value="">-- Select a Table --</option>'; // Reset
                data.tables.forEach(table => {
                    const option = document.createElement('option');
                    option.value = table;
                    option.textContent = table;
                    chTableSelect.appendChild(option);
                });
                chTableSelect.disabled = false;
                updateStatus('Tables loaded. Select a table.');
                 // Enable column loading if CH is source and table selected
                 enableActionButtons();
            } else {
                updateStatus(`Failed to load tables: ${data.error || 'Unknown error'}`, true);
                 chTableSelect.disabled = true;
            }
        } catch (error) {
            updateStatus(`Error loading tables: ${error}`, true);
             chTableSelect.disabled = true;
        }
         enableActionButtons(); // Re-enable buttons
    });

     // Handle Table Selection Change
     chTableSelect.addEventListener('change', () => {
         columnListDiv.innerHTML = '<p>Load columns for the selected table.</p>'; // Clear columns
         previewArea.style.display = 'none'; // Hide preview
         enableActionButtons(); // Enable Load Columns button if table selected
     });

    // Upload Flat File
    fileUploadInput.addEventListener('change', async () => {
        const file = fileUploadInput.files[0];
        if (!file) return;

        updateStatus(`Uploading ${file.name}...`);
        uploadStatusSpan.textContent = `Uploading ${file.name}...`;
        uploadedFilenameInput.value = ''; // Clear previous filename

        const formData = new FormData();
        formData.append('flatFile', file);

        try {
            const response = await fetch('/upload_flatfile', {
                method: 'POST',
                body: formData
            });
            const data = await response.json();
            if (response.ok && data.success) {
                updateStatus(`File '${data.filename}' uploaded successfully.`);
                 uploadStatusSpan.textContent = `Uploaded: ${data.filename}`;
                 uploadedFilenameInput.value = data.filename; // Store filename
                 columnListDiv.innerHTML = '<p>Load columns from the uploaded file.</p>'; // Clear columns
                 previewArea.style.display = 'none'; // Hide preview
                 enableActionButtons(); // Enable Load Columns button
            } else {
                updateStatus(`Upload failed: ${data.error || 'Unknown error'}`, true);
                uploadStatusSpan.textContent = `Upload failed.`;
            }
        } catch (error) {
            updateStatus(`Upload error: ${error}`, true);
            uploadStatusSpan.textContent = `Upload error.`;
        }
        enableActionButtons(); // Re-enable buttons
    });


    // Load Columns (from CH Table or Flat File)
    btnLoadColumns.addEventListener('click', async () => {
        updateStatus('Loading columns...');
        columnListDiv.innerHTML = 'Loading...'; // Indicate loading in the column area
         btnSelectAll.disabled = true;
         btnDeselectAll.disabled = true;
         previewArea.style.display = 'none'; // Hide preview

        const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
        const payload = { sourceType: sourceType };

        if (sourceType === 'clickhouse') {
            payload.clickhouseConfig = getClickHouseConfig();
            payload.tableName = chTableSelect.value;
            if (!payload.tableName) {
                 updateStatus('Please select a ClickHouse table first.', true);
                 columnListDiv.innerHTML = '<p>Select a table first.</p>';
                 enableActionButtons();
                 return;
            }
        } else if (sourceType === 'flatfile') {
            payload.filename = uploadedFilenameInput.value;
            payload.delimiter = document.getElementById('ff-delimiter').value;
             if (!payload.filename) {
                 updateStatus('Please upload a Flat File first.', true);
                 columnListDiv.innerHTML = '<p>Upload a file first.</p>';
                 enableActionButtons();
                 return;
            }
        } else {
            updateStatus('Invalid source type selected.', true);
             columnListDiv.innerHTML = '<p>Invalid source.</p>';
             enableActionButtons();
             return;
        }

        try {
            const response = await fetch('/get_columns', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (response.ok) {
                columnListDiv.innerHTML = ''; // Clear loading message
                if (data.columns && data.columns.length > 0) {
                     data.columns.forEach(col => {
                        const label = document.createElement('label');
                        label.style.display = 'block'; // One column per line
                        const checkbox = document.createElement('input');
                        checkbox.type = 'checkbox';
                        checkbox.name = 'selectedColumns';
                        checkbox.value = col;
                        checkbox.checked = true; // Default to selected
                        checkbox.onchange = enableActionButtons; // Re-check button states on change
                        label.appendChild(checkbox);
                        label.appendChild(document.createTextNode(` ${col}`));
                        columnListDiv.appendChild(label);
                    });
                    updateStatus('Columns loaded. Select columns to ingest.');
                     btnSelectAll.disabled = false;
                     btnDeselectAll.disabled = false;
                } else {
                     columnListDiv.innerHTML = '<p>No columns found or source is empty.</p>';
                     updateStatus('No columns found in the source.', true);
                }
                 enableActionButtons(); // Enable Preview/Ingest buttons
            } else {
                updateStatus(`Failed to load columns: ${data.error || 'Unknown error'}`, true);
                 columnListDiv.innerHTML = `<p class="error">Error: ${data.error || 'Unknown error'}</p>`;
            }
        } catch (error) {
            updateStatus(`Error loading columns: ${error}`, true);
            columnListDiv.innerHTML = `<p class="error">Error: ${error}</p>`;
        }
        enableActionButtons(); // Re-enable relevant buttons
    });

     // Select/Deselect All Columns
     btnSelectAll.addEventListener('click', () => {
         columnListDiv.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = true);
         enableActionButtons();
     });

     btnDeselectAll.addEventListener('click', () => {
         columnListDiv.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
         enableActionButtons();
     });


    // Preview Data
    btnPreview.addEventListener('click', async () => {
        updateStatus('Loading preview...');
        previewArea.style.display = 'none';
        previewTableContainer.innerHTML = 'Loading preview...';

        const selectedColumns = Array.from(columnListDiv.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
        if (selectedColumns.length === 0) {
            updateStatus('Please select columns to preview.', true);
            previewTableContainer.innerHTML = '';
            enableActionButtons();
            return;
        }

        const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
        const payload = {
            sourceType: sourceType,
            columns: selectedColumns
        };

        if (sourceType === 'clickhouse') {
            payload.clickhouseConfig = getClickHouseConfig();
            payload.tableName = chTableSelect.value;
        } else { // flatfile
            payload.filename = uploadedFilenameInput.value;
            payload.delimiter = document.getElementById('ff-delimiter').value;
        }

        try {
            const response = await fetch('/preview_data', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            const data = await response.json();

            if (response.ok) {
                previewArea.style.display = 'block';
                if (data.previewData && data.previewData.length > 0) {
                    // Generate HTML Table for Preview
                    let tableHTML = '<table><thead><tr>';
                    data.columns.forEach(col => tableHTML += `<th>${col}</th>`); // Use returned columns for header order
                    tableHTML += '</tr></thead><tbody>';

                    data.previewData.forEach(row => {
                        tableHTML += '<tr>';
                        // Ensure data is displayed in the same order as headers
                        data.columns.forEach(col => {
                             // Handle null/undefined gracefully
                             const value = row[col] !== null && row[col] !== undefined ? row[col] : '';
                             // Basic escaping for HTML display
                             const escapedValue = String(value).replace(/</g, "&lt;").replace(/>/g, "&gt;");
                             tableHTML += `<td>${escapedValue}</td>`;
                        });
                        tableHTML += '</tr>';
                    });

                    tableHTML += '</tbody></table>';
                    previewTableContainer.innerHTML = tableHTML;
                    updateStatus('Preview loaded (first 100 rows).');
                } else {
                    previewTableContainer.innerHTML = '<p>No data found for preview.</p>';
                    updateStatus('Preview loaded, but no data found.');
                }
            } else {
                updateStatus(`Failed to load preview: ${data.error || 'Unknown error'}`, true);
                previewTableContainer.innerHTML = `<p class="error">Error: ${data.error || 'Unknown error'}</p>`;
            }
        } catch (error) {
            updateStatus(`Error loading preview: ${error}`, true);
            previewTableContainer.innerHTML = `<p class="error">Error: ${error}</p>`;
        }
         enableActionButtons(); // Re-enable buttons
    });


    // Start Ingestion
    btnStartIngestion.addEventListener('click', async () => {
        updateStatus('Starting ingestion...');
         resultArea.textContent = ''; // Clear previous results
         errorArea.textContent = ''; // Clear previous errors
         downloadArea.innerHTML = ''; // Clear previous download link
         previewArea.style.display = 'none'; // Hide preview during ingestion

        const selectedColumns = Array.from(columnListDiv.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
        if (selectedColumns.length === 0) {
            updateStatus('Please select columns to ingest.', true);
            enableActionButtons();
            return;
        }

        const sourceType = document.querySelector('input[name="sourceType"]:checked').value;
        const targetType = document.querySelector('input[name="targetType"]:checked').value;

        const payload = {
            sourceType: sourceType,
            targetType: targetType,
            columns: selectedColumns
        };

        // Add specific configs based on source/target
        if (sourceType === 'clickhouse' || targetType === 'clickhouse') {
            payload.clickhouseConfig = getClickHouseConfig();
        }
        if (sourceType === 'flatfile' || targetType === 'flatfile') {
             payload.flatfileConfig = getFlatFileConfig(); // Includes filename (source) or output name (target) and delimiter
        }

        // Add source table name if source is ClickHouse
        if (sourceType === 'clickhouse') {
             payload.tableName = chTableSelect.value;
             if (!payload.tableName) {
                 updateStatus('ClickHouse source table not selected.', true);
                 enableActionButtons();
                 return;
            }
        }
         // Add source filename if source is FlatFile
         if (sourceType === 'flatfile') {
             payload.flatfileConfig.filename = uploadedFilenameInput.value; // Ensure source filename is included
             if (!payload.flatfileConfig.filename) {
                 updateStatus('Source flat file not uploaded or selected.', true);
                 enableActionButtons();
                 return;
            }
         }

        // Add target table name if target is ClickHouse
        if (targetType === 'clickhouse') {
            payload.targetTableName = document.getElementById('ch-target-table').value;
             if (!payload.targetTableName) {
                 updateStatus('Target ClickHouse table name is required.', true);
                 enableActionButtons();
                 return;
            }
        }


        try {
            updateStatus('Ingesting data... This may take a while for large datasets.');
            const response = await fetch('/start_ingestion', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            const data = await response.json();

            if (response.ok && data.success) {
                 updateStatus('Ingestion completed successfully!', false, true);
                 displayResult(data.message, data.filename, data.recordCount); // Display success message and potential download link
            } else {
                 updateStatus(`Ingestion failed: ${data.error || 'Unknown error'}`, true);
            }
        } catch (error) {
            updateStatus(`Ingestion error: ${error}`, true);
        }
        // Don't call enableActionButtons here, let displayResult handle it on success
        // or the error handling above will keep buttons disabled
         if (statusArea.textContent.includes('failed') || statusArea.textContent.includes('error')) {
            enableActionButtons(); // Re-enable buttons if there was an error
         }
    });

}); // End DOMContentLoaded
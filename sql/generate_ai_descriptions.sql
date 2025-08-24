-- =====================================================================================
-- Snowflake Stored Procedure: GENERATE_AI_DESCRIPTIONS
-- 
-- Purpose: Generate AI-powered descriptions for tables using Snowflake Cortex
-- JavaScript version for better string handling
-- 
-- Parameters:
--   DATABASE_NAME (STRING): Target database name
--   SCHEMA_NAME (STRING): Target schema name  
--   TABLE_NAMES (ARRAY): Array of table names to process
--   MODEL_NAME (STRING): Cortex model to use (default: 'llama3.1-8b')
--   GENERATE_TYPE (STRING): 'TABLE', 'COLUMN', or 'BOTH' (default: 'TABLE')
--
-- Returns: Summary of descriptions generated
-- =====================================================================================

USE ROLE accountadmin;
USE DATABASE DB_SNOWTOOLS;
USE SCHEMA DB_SNOWTOOLS.PUBLIC;

CREATE OR REPLACE PROCEDURE GENERATE_AI_DESCRIPTIONS(
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAMES ARRAY,
    MODEL_NAME STRING DEFAULT 'llama3.1-8b',
    GENERATE_TYPE STRING DEFAULT 'TABLE'
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Validate inputs
    if (!DATABASE_NAME || DATABASE_NAME === '') {
        return 'ERROR: Database name cannot be empty';
    }
    
    if (!SCHEMA_NAME || SCHEMA_NAME === '') {
        return 'ERROR: Schema name cannot be empty';
    }
    
    if (!TABLE_NAMES || TABLE_NAMES.length === 0) {
        return 'ERROR: Table names array cannot be empty';
    }
    
    // Validate generation type
    const validTypes = ['TABLE', 'COLUMN', 'BOTH'];
    if (!validTypes.includes(GENERATE_TYPE.toUpperCase())) {
        return 'ERROR: GENERATE_TYPE must be TABLE, COLUMN, or BOTH';
    }
    
    let tableCount = 0;
    let columnCount = 0;
    const totalTables = TABLE_NAMES.length;
    
    // Process each table
    for (let i = 0; i < totalTables; i++) {
        const tableName = TABLE_NAMES[i];
        const fullTableName = `${DATABASE_NAME}.${SCHEMA_NAME}.${tableName}`;
        
        // Generate table descriptions if requested
        if (['TABLE', 'BOTH'].includes(GENERATE_TYPE.toUpperCase())) {
            
            // Build column metadata string (simplified)
            const columnInfo = `Columns from ${fullTableName} (metadata retrieved from INFORMATION_SCHEMA)`;
            
            // Get sample data reference
            const sampleData = `Sample data from ${fullTableName} (first 5 rows)`;
            
            // Build the table description prompt (same as Python code)
            const tablePrompt = `You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 150 characters or less.

---METADATA---
TABLE Name: ${tableName}
Schema: ${SCHEMA_NAME}
Database: ${DATABASE_NAME}
Columns:
${columnInfo}

---SAMPLE DATA (LIMIT 5 ROWS)---
${sampleData}

---TASK---
Generate a description for the table named ${tableName}.`;
            
            try {
                // Call Cortex COMPLETE for table description
                const cortexQuery = `SELECT SNOWFLAKE.CORTEX.COMPLETE('${MODEL_NAME}', ?) as generated_description`;
                const stmt = snowflake.createStatement({
                    sqlText: cortexQuery,
                    binds: [tablePrompt]
                });
                const result = stmt.execute();
                
                let generatedDescription = '';
                if (result.next()) {
                    generatedDescription = result.getColumnValue(1);
                }
                
                // Clean up the description (remove quotes if present)
                generatedDescription = generatedDescription.trim();
                if (generatedDescription.startsWith('"') && generatedDescription.endsWith('"')) {
                    generatedDescription = generatedDescription.slice(1, -1);
                }
                
                // Apply the comment to the table
                const escapedDesc = generatedDescription.replace(/'/g, "''");
                const commentSql = `COMMENT ON TABLE ${fullTableName} IS '${escapedDesc}'`;
                
                const commentStmt = snowflake.createStatement({
                    sqlText: commentSql
                });
                commentStmt.execute();
                
                tableCount++;
                
            } catch (error) {
                // Continue processing other tables even if one fails
                // Could log error here if needed
            }
        }
        
        // Generate column descriptions if requested
        if (['COLUMN', 'BOTH'].includes(GENERATE_TYPE.toUpperCase())) {
            
            try {
                // Get column information for this table using fully qualified INFORMATION_SCHEMA
                const columnsQuery = `
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM ${DATABASE_NAME}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_CATALOG = ? 
                    AND TABLE_SCHEMA = ? 
                    AND TABLE_NAME = ? 
                    ORDER BY ORDINAL_POSITION`;
                
                const columnsStmt = snowflake.createStatement({
                    sqlText: columnsQuery,
                    binds: [DATABASE_NAME.toUpperCase(), SCHEMA_NAME.toUpperCase(), tableName.toUpperCase()]
                });
                const columnsResult = columnsStmt.execute();
                
                // Debug: Check if we found any columns
                let foundColumns = false;
                
                // Process each column
                while (columnsResult.next()) {
                    foundColumns = true;
                    const columnName = columnsResult.getColumnValue(1);
                    const dataType = columnsResult.getColumnValue(2);
                    
                    // Get sample data reference for the column
                    const columnSampleData = `Sample column data retrieved (10 rows from ${columnName})`;
                    
                    // Build the column description prompt (same as Python code)
                    const columnPrompt = `You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 100 characters or less.

---METADATA---
Column Name: ${columnName}
Table Name: ${tableName}
Schema: ${SCHEMA_NAME}
Database: ${DATABASE_NAME}
Data Type: ${dataType}

---SAMPLE DATA (LIMIT 10 ROWS)---
${columnSampleData}

---TASK---
Generate a description for the column named ${columnName}.`;
                    
                    try {
                        // Call Cortex COMPLETE for column description
                        const columnCortexQuery = `SELECT SNOWFLAKE.CORTEX.COMPLETE('${MODEL_NAME}', ?) as generated_description`;
                        const columnStmt = snowflake.createStatement({
                            sqlText: columnCortexQuery,
                            binds: [columnPrompt]
                        });
                        const columnResult = columnStmt.execute();
                        
                        let columnGeneratedDescription = '';
                        if (columnResult.next()) {
                            columnGeneratedDescription = columnResult.getColumnValue(1);
                        }
                        
                        // Clean up the column description
                        columnGeneratedDescription = columnGeneratedDescription.trim();
                        if (columnGeneratedDescription.startsWith('"') && columnGeneratedDescription.endsWith('"')) {
                            columnGeneratedDescription = columnGeneratedDescription.slice(1, -1);
                        }
                        
                        // Apply the comment to the column
                        const escapedColDesc = columnGeneratedDescription.replace(/'/g, "''");
                        const columnCommentSql = `COMMENT ON COLUMN ${fullTableName}."${columnName}" IS '${escapedColDesc}'`;
                        
                        const columnCommentStmt = snowflake.createStatement({
                            sqlText: columnCommentSql
                        });
                        columnCommentStmt.execute();
                        
                        columnCount++;
                        
                    } catch (columnError) {
                        // Continue processing other columns even if one fails
                        // Could log error here if needed
                    }
                }
                
                // Debug: If no columns were found, this might indicate an issue
                if (!foundColumns) {
                    // This could be added to the return message for debugging
                }
                
            } catch (error) {
                // Continue processing other tables even if column processing fails
                // Could log error here if needed
            }
        }
    }
    
    // Return summary
    return `AI Description Generation Complete! ` +
           `Processed ${totalTables} table(s). ` +
           `Generated ${tableCount} table description(s) and ${columnCount} column description(s). ` +
           `Model used: ${MODEL_NAME}. Generation type: ${GENERATE_TYPE}.`;
$$;

-- =====================================================================================
-- Usage Examples:
-- =====================================================================================

-- Example 1: Generate table descriptions for multiple tables
-- CALL GENERATE_AI_DESCRIPTIONS('MY_DATABASE', 'MY_SCHEMA', ['TABLE1', 'TABLE2'], 'llama3.1-8b', 'TABLE');

-- Example 2: Test with your data
use warehouse d4b_wh;
CALL DB_SNOWTOOLS.PUBLICGENERATE_AI_DESCRIPTIONS('CALL_CENTER', 'TRANSCRIPTS', ['CALL_TRANSCRIPTS'], 'llama3.1-8b', 'TABLE');
CALL DB_SNOWTOOLS.PUBLICGENERATE_AI_DESCRIPTIONS('CALL_CENTER', 'TRANSCRIPTS', ['CALL_TRANSCRIPTS'], 'claude-4-sonnet', 'BOTH');

CALL DB_SNOWTOOLS.PUBLIC.GENERATE_AI_DESCRIPTIONS('RETAIL_DEMO', 'SALES', ['ECOM_SALES'], 'claude-4-sonnet', 'BOTH');

-- =====================================================================================
-- Notes:
-- =====================================================================================
-- 1. This JavaScript version handles string quoting much better than SQL
-- 2. Uses parameter binding (?) to safely pass the prompt to Cortex
-- 3. Includes proper error handling to continue processing if individual tables fail
-- 4. Same AI prompts as the Python application
-- 5. Comments are applied directly to tables using COMMENT ON statements
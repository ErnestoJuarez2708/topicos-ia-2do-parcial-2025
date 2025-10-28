import os
import sqlite3
import csv
from datetime import datetime
import ast


def _auto_save_to_global_csv(query: str, result: str) -> None:
    """
    Internal function that automatically saves SELECT query results to the global CSV.
    This runs after every execute_sql() call that returns data.
    """
    try:
        # Only save if it's a SELECT query with data
        if not query.strip().upper().startswith('SELECT'):
            return
        
        # Try to parse the result as data
        try:
            data = ast.literal_eval(result)
            if not data or not isinstance(data, list):
                return
        except:
            return  # Not parseable data, skip
        
        # Save to global CSV
        global_file = "query_results.csv"
        abs_path = os.path.abspath(global_file)
        file_exists = os.path.exists(abs_path)
        
        with open(abs_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            if file_exists:
                writer.writerow([])  # Separator
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([f"=== AUTO-SAVED Query at: {timestamp} ==="])
            writer.writerow([f"SQL: {query[:100]}..."])  # First 100 chars of query
            writer.writerows(data)
        
        print(f"   [Auto-Save] Query results saved to {global_file}")
    except:
        pass  # Silently fail, don't interrupt the main flow


def execute_sql(conn: sqlite3.Connection, query: str, query_history: list[str] | None = None) -> str:
    """
    Executes a SQL query.
    Automatically saves SELECT query results to the global CSV file.
    Returns the fetched data as a string or the error message as a string.
    """
    print(f"   [BEGIN Tool Action] Executing SQL: {query} [END Tool Action]")
    if query_history is not None:
        query_history.append(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        # Check if it was a query that returns data (SELECT)
        if cursor.description:
            rows = cursor.fetchall()
            result = str(rows)
            
            # Auto-save to global CSV
            _auto_save_to_global_csv(query, result)
            
            return result  # Return data as a string
        else:
            conn.commit()
            return "Query executed successfully (no data returned)."
    except sqlite3.Error as e:
        print(f"   [ERROR] {e}")
        return f"Error: {e}"  # Return the error message string


def get_schema(conn: sqlite3.Connection, table_name: str | None = None) -> str:
    """
    Gets the schema for all tables or a specific table.
    """
    print(f"   [Tool Action] Getting schema for: {table_name or 'all tables'}")
    cursor = conn.cursor()
    if table_name:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        return str([(col[1], col[2]) for col in columns])
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        return str([table[0] for table in tables])


def save_data_to_csv(data: list[tuple], filename: str = "", query_description: str = "") -> str:
    """
    Saves tabular data to a specific CSV file when the user explicitly requests it.
    This creates individual CSV files per user request.
    
    Args:
        data: A list of tuples or lists containing the data rows to save.
        filename: The name of the CSV file to create. If empty, uses a timestamped name.
        query_description: Optional description to add as a header in the file.
    
    Returns:
        A success message with the absolute file path, or an error message if the operation fails.
    """
    print(f"   [Tool Action] Creating individual CSV file: {filename or 'auto-named'}...")
    
    try:
        # Validate input data
        if not data:
            return "Error: No data provided. The data list is empty."
        
        if not isinstance(data, list):
            return f"Error: Data must be a list, received {type(data).__name__}."
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_{timestamp}.csv"
        
        # Ensure filename has .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Get the absolute path
        abs_path = os.path.abspath(filename)
        
        # Create directory if it doesn't exist
        directory = os.path.dirname(abs_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        
        # Write data to CSV file (OVERWRITE mode for individual files)
        with open(abs_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Add optional header with description
            if query_description:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([f"Generated at: {timestamp}"])
                writer.writerow([f"Description: {query_description}"])
                writer.writerow([])  # Empty line
            
            # Write the actual data
            writer.writerows(data)
        
        return f"Success: Data saved to individual file {abs_path}"
    
    except PermissionError:
        return f"Error: Permission denied. Cannot write to {filename}."
    except OSError as e:
        return f"Error: OS error occurred - {str(e)}"
    except Exception as e:
        return f"Error: Failed to save data - {type(e).__name__}: {str(e)}"
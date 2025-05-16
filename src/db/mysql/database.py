import re
import mysql.connector
import pandas as pd
from src.logger import Logger
from typing import Dict, List, Optional, Any, Tuple


class MySQLDBManager:
    """Class for managing database connections to MySQL."""
    
    def __init__(self, logger: Logger, connection_params: Dict[str, Any] = None):
        """
        Initialize the database manager with connection parameters.
        
        Args:
            logger (Logger): Logger instance for logging operations
            connection_params (Dict[str, Any], optional): Dictionary of connection parameters
        """
        self.logger = logger
        self.connection_params = connection_params or {}
        # Set default timeout values if not provided
        if 'connect_timeout' not in self.connection_params:
            self.connection_params['connect_timeout'] = 60  # Default 60 seconds for connection timeout
        self.connection = None
        self.table_schema = {}
    
    def connect(self, 
                host: str = None, 
                port: int = None, 
                database: str = None, 
                user: str = None, 
                password: str = None,
                connect_timeout: int = None,
                statement_timeout: int = None) -> bool:
        """
        Connect to MySQL database.
        
        Args:
            host (str, optional): Database host
            port (int, optional): Database port
            database (str, optional): Database name
            user (str, optional): Database user
            password (str, optional): Database password
            connect_timeout (int, optional): Connection timeout in seconds
            statement_timeout (int, optional): Statement timeout in milliseconds
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        # Update connection parameters if provided
        if host:
            self.connection_params["host"] = host
        if port:
            self.connection_params["port"] = port
        if database:
            self.connection_params["database"] = database
        if user:
            self.connection_params["user"] = user
        if password:
            self.connection_params["password"] = password
        if connect_timeout:
            self.connection_params["connect_timeout"] = connect_timeout
            
        try:
            self.connection = mysql.connector.connect(**self.connection_params)
            self.logger.add_log(f"Database connection successful - Host: {self.connection_params.get('host')}")
            
            # Set session parameters for all connections
            with self.connection.cursor() as cursor:
                # Set timeouts for MySQL
                if statement_timeout:
                    # Convert milliseconds to seconds for MySQL
                    timeout_seconds = statement_timeout / 1000
                    try:
                        cursor.execute(f"SET max_execution_time = {int(statement_timeout)};")  # MySQL specific timeout in milliseconds
                    except mysql.connector.Error as e:
                        if "Unknown system variable" in str(e):
                            self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                            cursor.execute(f"SET SESSION wait_timeout = {int(timeout_seconds)};")
                            cursor.execute(f"SET SESSION interactive_timeout = {int(timeout_seconds)};")
                        else:
                            raise
                    cursor.execute(f"SET interactive_timeout = {int(timeout_seconds)};")  # MySQL session timeout in seconds
                    cursor.execute(f"SET wait_timeout = {int(timeout_seconds)};")  # MySQL connection timeout in seconds
                else:
                    try:
                        cursor.execute("SET max_execution_time = 300000;")  # 5 minutes
                    except mysql.connector.Error as e:
                        if "Unknown system variable" in str(e):
                            self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                            cursor.execute("SET SESSION wait_timeout = 300;")  # 5 minutes
                            cursor.execute("SET SESSION interactive_timeout = 300;")  # 5 minutes
                        else:
                            raise
                    cursor.execute("SET interactive_timeout = 300;")  # 5 minutes
                    cursor.execute("SET wait_timeout = 300;")  # 5 minutes
                
                self.connection.commit()
            
            return True
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"Database connection failed: {error_msg}")
            return False
    
    def disconnect(self) -> None:
        """Close the database connection if it exists."""
        if self.connection:
            self.connection.close()
            self.logger.add_log("Database connection closed")
            self.connection = None
    
    def execute_query(self, query: str, params: tuple = None, timeout: int = None) -> Optional[list]:
        """
        Execute a READ-ONLY SQL query and return results.
        Only allows SELECT statements and other read-only operations.
        Blocks any attempt to modify the database (INSERT, UPDATE, DELETE, etc.).
        
        Args:
            query (str): SQL query to execute (must be read-only)
            params (tuple, optional): Parameters for the query
            timeout (int, optional): Override statement timeout for this query in milliseconds
            
        Returns:
            Optional[list]: Query results or None if failed or blocked
        """
        if not self.connection:
            self.logger.add_log("Query execution failed: No active database connection")
            return None
            
        # Normalize query for safety checks
        normalized_query = query.strip().upper()
        try:
            # Check if query is safe
            if not self.is_read_only_query(normalized_query) or self.contains_unsafe_operations(normalized_query):
                self.logger.add_log(f"Query blocked: Non-read operation detected in query: {query[:50]}{'...' if len(query) > 50 else ''}")
                return {"error": "Operation blocked - Non-read operation detected in query"}
                
            # If we got here, the query is safe to execute
            cursor = self.connection.cursor()
            
            # Set a specific timeout for this query if requested
            if timeout:
                try:
                    cursor.execute(f"SET max_execution_time = {timeout};")
                except mysql.connector.Error as e:
                    if "Unknown system variable" in str(e):
                        self.logger.add_log(f"max_execution_time not supported, using statement timeout settings instead")
                        cursor.execute(f"SET SESSION wait_timeout = {timeout // 1000 + 1};")
                        cursor.execute(f"SET SESSION interactive_timeout = {timeout // 1000 + 1};")
                    else:
                        raise
            
            cursor.execute(query, params or ())
            
            try:
                results = cursor.fetchall()
                self.logger.add_log(f"Read query executed successfully: {query[:50]}{'...' if len(query) > 50 else ''}")
                return results
            except mysql.connector.errors.InterfaceError as e:
                if "No result set to fetch from" in str(e):
                    self.logger.add_log(f"Query executed but returned no results: {query[:50]}{'...' if len(query) > 50 else ''}")
                    return []
                else:
                    self.logger.add_log(f"Unexpected error with read-only query: {str(e)}")
                    self.connection.rollback()
                    return {"error": f"Unexpected error with read-only query: {str(e)}"}
                    
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"Query execution failed: {error_msg}")
            self.connection.rollback()
            return {"error": f"Query execution failed: {error_msg}"}
    
    def is_read_only_query(self, sql: str) -> bool:
        """Determine if a SQL query is read-only (SELECT or other safe read operations)"""
        if sql.startswith("SELECT"):
            return True
            
        safe_prefixes = [
            "EXPLAIN ",
            "SHOW ",
            "DESCRIBE ",
            "WITH ",
        ]
        
        for prefix in safe_prefixes:
            if sql.startswith(prefix):
                if prefix == "WITH ":
                    unsafe_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
                    return not any(keyword in sql for keyword in unsafe_keywords)
                return True
                
        return False
    
    def contains_unsafe_operations(self, sql: str) -> bool:
        """Check if SQL contains any operations that could modify the database"""
        unsafe_keywords = [
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
            "GRANT", "REVOKE", "OPTIMIZE", "REPAIR", "ANALYZE", 
            "CALL", "DO", "LOCK", "UNLOCK",
            "PREPARE", "DEALLOCATE", "SAVEPOINT", "RELEASE",
            "COMMIT", "ROLLBACK", "START", "BEGIN", "END", "XA",
            "FLUSH", "RESET", "PURGE", "CHANGE", "SHUTDOWN",
            "KILL", "LOAD", "HANDLER"
        ]
        
        unsafe_functions = [
            "SLEEP", "BENCHMARK", "LOAD_FILE", "FOUND_ROWS", "DATABASE", 
            "USER", "SYSTEM_USER", "SESSION_USER", "PASSWORD", "ENCRYPT", 
            "COMPRESS", "ENCODE", "DECODE"
        ]
        
        for keyword in unsafe_keywords:
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, sql):
                return True
                
        for func in unsafe_functions:
            pattern = r'\b' + func + r'\s*\('
            if re.search(pattern, sql):
                return True
                
        return False
    
    def get_table_schema(self) -> bool:
        """
        Get schema information for all tables in the database.
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.add_log("Retrieving table schema...")
        
        if not self.connection:
            self.logger.add_log("Database not connected")
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Set a longer timeout for schema operations
            try:
                cursor.execute("SET max_execution_time = 600000;")  # 10 minutes
            except mysql.connector.Error as e:
                if "Unknown system variable" in str(e):
                    self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                    cursor.execute("SET SESSION wait_timeout = 600;")  # 10 minutes
                    cursor.execute("SET SESSION interactive_timeout = 600;")  # 10 minutes
                else:
                    raise
            
            # Get list of tables - MySQL specific
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_type = 'BASE TABLE'
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get columns for each table
            for table in tables:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                    AND table_name = '{table}'
                """)
                
                columns = [{"name": row[0], "type": row[1], "nullable": row[2]} for row in cursor.fetchall()]
                self.table_schema[table] = columns
                
            cursor.close()
            self.logger.add_log(f"✅ Retrieved schema for {len(tables)} tables")
            return True
            
        except Exception as e:
            self.logger.add_log(f"❌ Error retrieving table schema: {e}")
            return False
    
    def execute_query_to_dataframe(self, query: str, params: Optional[Dict[str, Any]] = None, timeout: int = None) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return results as DataFrame.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            timeout: Override statement timeout for this query in milliseconds
            
        Returns:
            DataFrame containing query results or None if failed
        """
        if not self.connection:
            self.logger.add_log("Database not connected")
            return None
            
        try:
            # Set a specific timeout for this query if requested
            if timeout:
                with self.connection.cursor() as cursor:
                    try:
                        cursor.execute(f"SET max_execution_time = {timeout};")
                    except mysql.connector.Error as e:
                        if "Unknown system variable" in str(e):
                            self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                            cursor.execute(f"SET SESSION wait_timeout = {timeout // 1000 + 1};")
                            cursor.execute(f"SET SESSION interactive_timeout = {timeout // 1000 + 1};")
                        else:
                            raise
                    self.connection.commit()
            
            if params:
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute(query, params)
                result = pd.DataFrame(cursor.fetchall())
                cursor.close()
            else:
                result = pd.read_sql(query, self.connection)
                
            self.logger.add_log(f"Query to DataFrame executed successfully: {query[:50]}{'...' if len(query) > 50 else ''}")
            return result
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"❌ Query to DataFrame execution error: {error_msg}")
            return None
            
    def get_table_relationships(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get foreign key relationships between tables.
        
        Returns:
            Dictionary of table relationships
        """
        if not self.connection:
            self.logger.add_log("Database not connected")
            return {}
            
        try:
            cursor = self.connection.cursor()
            
            # Set a longer timeout for schema operations
            try:
                cursor.execute("SET max_execution_time = 600000;")  # 10 minutes
            except mysql.connector.Error as e:
                if "Unknown system variable" in str(e):
                    self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                    cursor.execute("SET SESSION wait_timeout = 600;")  # 10 minutes
                    cursor.execute("SET SESSION interactive_timeout = 600;")  # 10 minutes
                else:
                    raise
            
            cursor.execute("""
                SELECT
                    TABLE_NAME AS table_name,
                    COLUMN_NAME AS column_name,
                    REFERENCED_TABLE_NAME AS foreign_table_name,
                    REFERENCED_COLUMN_NAME AS foreign_column_name
                FROM
                    information_schema.KEY_COLUMN_USAGE
                WHERE
                    REFERENCED_TABLE_SCHEMA = DATABASE()
                    AND REFERENCED_TABLE_NAME IS NOT NULL;
            """)
            
            relationships = {}
            for row in cursor.fetchall():
                table_name, column_name, foreign_table, foreign_column = row
                if table_name not in relationships:
                    relationships[table_name] = []
                    
                relationships[table_name].append({
                    "column": column_name,
                    "references_table": foreign_table,
                    "references_column": foreign_column
                })
                
            cursor.close()
            self.logger.add_log("Retrieved table relationships successfully")
            return relationships
            
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"❌ Error retrieving table relationships: {error_msg}")
            return {}
            
    def get_rich_schema_info(self) -> Dict[str, Any]:
        """
        Get enriched schema information including relationships, indexes, etc.
        
        Returns:
            Dictionary with comprehensive schema information
        """
        self.logger.add_log("Retrieving rich schema information...")
        
        try:
            schema_info = {
                "tables": self.table_schema,
                "relationships": self.get_table_relationships()
            }
            
            if self.connection:
                indexes = {}
                
                with self.connection.cursor() as cursor:
                    # Set a longer timeout for schema operations
                    try:
                        cursor.execute("SET max_execution_time = 600000;")  # 10 minutes
                    except mysql.connector.Error as e:
                        if "Unknown system variable" in str(e):
                            self.logger.add_log(f"max_execution_time not supported, using session timeout settings instead")
                            cursor.execute("SET SESSION wait_timeout = 600;")  # 10 minutes
                            cursor.execute("SET SESSION interactive_timeout = 600;")  # 10 minutes
                        else:
                            raise
                    
                    cursor.execute("""
                        SELECT
                            TABLE_NAME AS table_name,
                            INDEX_NAME AS index_name,
                            COLUMN_NAME AS column_name,
                            NOT NON_UNIQUE AS is_unique
                        FROM
                            information_schema.STATISTICS
                        WHERE
                            TABLE_SCHEMA = DATABASE()
                        ORDER BY
                            TABLE_NAME,
                            INDEX_NAME;
                    """)
                    
                    for row in cursor.fetchall():
                        table_name, index_name, column_name, is_unique = row
                        if table_name not in indexes:
                            indexes[table_name] = []
                            
                        index_exists = False
                        for index in indexes[table_name]:
                            if index["name"] == index_name:
                                index["columns"].append(column_name)
                                index_exists = True
                                break
                                
                        if not index_exists:
                            indexes[table_name].append({
                                "name": index_name,
                                "columns": [column_name],
                                "unique": is_unique
                            })
                
                schema_info["indexes"] = indexes
                self.logger.add_log("Rich schema info retrieved successfully")
            
            return schema_info
            
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"❌ Error retrieving rich schema info: {error_msg}")
            return {"tables": self.table_schema, "relationships": {}, "error": str(e)}
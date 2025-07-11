import re
import psycopg2
import pandas as pd
from src.logger import Logger
from typing import Dict, List, Optional, Any, Tuple


class PostgresDBManager:
    """Class for managing database connections to PostgreSQL."""
    
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
        if 'options' not in self.connection_params:
            self.connection_params['options'] = '-c statement_timeout=300000'  # Default 5 minutes for query execution
        self.connection = None
        self.table_schema = {}

    
    def connect(self, 
                host: str = None, 
                port: int = None, 
                dbname: str = None, 
                user: str = None, 
                password: str = None,
                connect_timeout: int = None,
                statement_timeout: int = None) -> bool:
        """
        Connect to PostgreSQL database.
        
        Args:
            host (str, optional): Database host
            port (int, optional): Database port
            dbname (str, optional): Database name
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
        if dbname:
            self.connection_params["dbname"] = dbname
        if user:
            self.connection_params["user"] = user
        if password:
            self.connection_params["password"] = password
        if connect_timeout:
            self.connection_params["connect_timeout"] = connect_timeout
        if statement_timeout:
            self.connection_params["options"] = f'-c statement_timeout={statement_timeout}'
            
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.logger.add_log(f"Database Postgres connection successful - Host:")
            
            # Set session parameters for all connections
            with self.connection.cursor() as cursor:
                # Set idle_in_transaction_session_timeout to prevent idle sessions
                cursor.execute("SET idle_in_transaction_session_timeout = '300000';")  # 5 minutes
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
                return {"error": "Operation blocked -  Non-read operation detected in query"}

                
            # If we got here, the query is safe to execute
            cursor = self.connection.cursor()
            
            # Set a specific timeout for this query if requested
            if timeout:
                cursor.execute(f"SET statement_timeout = {timeout};")
            
            cursor.execute(query, params or ())
            
            try:
                results = cursor.fetchall()
                self.logger.add_log(f"Read query executed successfully: {query[:50]}{'...' if len(query) > 50 else ''}")
                return results
            except psycopg2.ProgrammingError as e:
                # This should not happen with properly filtered read-only queries
                # But handle it gracefully just in case
                self.logger.add_log(f"Unexpected error with read-only query: {str(e)}")
                self.connection.rollback()
                return {"error": f"Unexpected error with read-only query: {str(e)}"}

                    
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"Query execution failed: {error_msg}")
            self.connection.rollback()
            return {"error": f"Query execution failed: {error_msg}"}

    
    # Check if query is read-only
    def is_read_only_query(self, sql: str) -> bool:
        """Determine if a SQL query is read-only (SELECT or other safe read operations)"""
        # Check if query starts with SELECT
        if sql.startswith("SELECT"):
            return True
            
        # Other safe read-only operations
        safe_prefixes = [
            "EXPLAIN ",
            "SHOW ",
            "DESCRIBE ",
            "WITH ", # CTEs - need further inspection
        ]
        
        for prefix in safe_prefixes:
            if sql.startswith(prefix):
                # For WITH queries, we need to ensure they don't contain data modification
                if prefix == "WITH ":
                    # Check for INSERT/UPDATE/DELETE in the CTE
                    unsafe_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
                    return not any(keyword in sql for keyword in unsafe_keywords)
                return True
                
        return False
    
    # Check for unsafe operations
    def contains_unsafe_operations(self, sql: str) -> bool:
        """Check if SQL contains any operations that could modify the database"""
        unsafe_keywords = [
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
            "GRANT", "REVOKE", "VACUUM", "CLUSTER", "REINDEX", 
            "EXECUTE", "CALL", "DO", "LOCK", "UNLISTEN", "NOTIFY",
            "SECURITY LABEL", "PREPARE", "DEALLOCATE", "SAVEPOINT", "RELEASE",
            "COMMIT", "ROLLBACK", "BEGIN", "START", "END", "CHECKPOINT",
            "DECLARE", "FETCH", "MOVE", "CLOSE", "LISTEN", "UNLOCK",
            "ANALYZE", "LOAD", "COPY"
        ]
        
        # Check for unsafe functions
        unsafe_functions = [
            "PG_SLEEP", "PG_READ_FILE", "PG_EXECUTE", "LO_IMPORT", "LO_EXPORT",
            "PG_TERMINATE_BACKEND", "PG_RELOAD_CONF", "PG_ROTATE_LOGFILE"
        ]
        
        for keyword in unsafe_keywords:
            # Match whole words only
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, sql):
                return True
                
        for func in unsafe_functions:
            # Match function calls
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
            
            # Set a longer timeout for schema operations which might be slow on large databases
            cursor.execute("SET statement_timeout = 600000;")  # 10 minutes
            
            # Get list of tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get columns for each table
            for table in tables:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
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
            # Create a new connection with specified timeout if needed
            if timeout:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"SET statement_timeout = {timeout};")
                    self.connection.commit()
            
            result = pd.read_sql(query, self.connection, params=params)
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
            cursor.execute("SET statement_timeout = 600000;")  # 10 minutes
            
            cursor.execute("""
                SELECT
                    tc.table_name AS table_name, 
                    kcu.column_name AS column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY';
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
            
            # Get index information
            if self.connection:
                indexes = {}
                
                with self.connection.cursor() as cursor:
                    # Set a longer timeout for schema operations
                    cursor.execute("SET statement_timeout = 600000;")  # 10 minutes
                    
                    cursor.execute("""
                        SELECT
                            t.relname AS table_name,
                            i.relname AS index_name,
                            a.attname AS column_name,
                            ix.indisunique AS is_unique
                        FROM
                            pg_class t,
                            pg_class i,
                            pg_index ix,
                            pg_attribute a
                        WHERE
                            t.oid = ix.indrelid
                            AND i.oid = ix.indexrelid
                            AND a.attrelid = t.oid
                            AND a.attnum = ANY(ix.indkey)
                            AND t.relkind = 'r'
                            AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                        ORDER BY
                            t.relname,
                            i.relname;
                    """)
                    
                    for row in cursor.fetchall():
                        table_name, index_name, column_name, is_unique = row
                        if table_name not in indexes:
                            indexes[table_name] = []
                            
                        # Check if this index is already in our list
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
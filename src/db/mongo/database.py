"""
MongoDB database connection and query management.
"""
import re
import pymongo
import pandas as pd
from src.logger import Logger
from typing import Dict, List, Optional, Any, Union
from bson import json_util
import json


class MongoDBManager:
    """Class for managing database connections to MongoDB."""
    
    def __init__(self, logger: Logger, connection_params: Dict[str, Any] = None):
        """
        Initialize the MongoDB manager with connection parameters.
        
        Args:
            logger (Logger): Logger instance for logging operations
            connection_params (Dict[str, Any], optional): Dictionary of connection parameters
        """
        self.logger = logger
        self.connection_params = connection_params or {}
        # Set default timeout values if not provided
        if 'connectTimeoutMS' not in self.connection_params:
            self.connection_params['connectTimeoutMS'] = 60000  # Default 60 seconds for connection timeout
        if 'socketTimeoutMS' not in self.connection_params:
            self.connection_params['socketTimeoutMS'] = 300000  # Default 5 minutes for operation timeout
        self.client = None
        self.db = None
        self.collection_schema = {}

    def connect(self, 
                connection_string: str = None, 
                db_name: str = None,
                host: str = None, 
                port: int = None, 
                username: str = None, 
                password: str = None,
                connect_timeout_ms: int = None,
                socket_timeout_ms: int = None) -> bool:
        """
        Connect to MongoDB database.
        
        Args:
            connection_string (str, optional): MongoDB connection string URI
            db_name (str, optional): Database name
            host (str, optional): Database host
            port (int, optional): Database port
            username (str, optional): Database user
            password (str, optional): Database password
            connect_timeout_ms (int, optional): Connection timeout in milliseconds
            socket_timeout_ms (int, optional): Socket timeout in milliseconds
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # If connection string is provided, use it directly

            if connection_string:
                if connect_timeout_ms:
                    self.connection_params["connectTimeoutMS"] = connect_timeout_ms
                if socket_timeout_ms:
                    self.connection_params["socketTimeoutMS"] = socket_timeout_ms
                
                self.client = pymongo.MongoClient(connection_string, **self.connection_params)

                # Extract database name from connection string if not provided separately
                if not db_name and "/" in connection_string:
                    parts = connection_string.split("/")
                    if len(parts) > 3:  # Format: mongodb://host:port/dbname
                        extracted_db_name = parts[3].split("?")[0]  # Remove query parameters if present
                        db_name = extracted_db_name

            else:
                # Update connection parameters if provided
                if host:
                    self.connection_params["host"] = host
                if port:
                    self.connection_params["port"] = port
                if username:
                    self.connection_params["username"] = username
                if password:
                    self.connection_params["password"] = password
                if connect_timeout_ms:
                    self.connection_params["connectTimeoutMS"] = connect_timeout_ms
                if socket_timeout_ms:
                    self.connection_params["socketTimeoutMS"] = socket_timeout_ms
                
                self.client = pymongo.MongoClient(**self.connection_params)
                self.logger.add_log(f"LLL {self.client}")

            # Test connection with a ping command
            self.client.admin.command('ping')
            
            # Set the database
            if db_name:
                self.db = self.client[db_name]
                self.logger.add_log(f"Database selected: {db_name}")
            
            self.logger.add_log(f"MongoDB connection successful -")
            return True
            
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"MongoDB connection failed: {error_msg}")
            return False
    
    def disconnect(self) -> None:
        """Close the MongoDB connection if it exists."""
        if self.client:
            self.client.close()
            self.logger.add_log("MongoDB connection closed")
            self.client = None
            self.db = None
    
    def execute_query(self, raw_command: Union[str, Dict[str, Any]]) -> Any:
        """
        Executes a dynamic MongoDB command or query string for READ-ONLY operations.
        
        Supports both dictionary-based commands and MongoDB shell-style syntax.
        Any commands that would modify the database (create, update, delete) are blocked.

        Args:
            raw_command (str | dict): The read-only command/query to execute. Can be:
                - String like 'show collections'
                - MongoDB shell syntax like 'db.users.find()'
                - Dict for commands like {'find': 'users', 'filter': {...}}

        Returns:
            Any: Query results or metadata depending on the command.
        """
        if self.client is None or self.db is None:
            self.logger.add_log("❌ No active MongoDB connection.")
            return None

        try:
            # Define safe operations
            SAFE_STRING_COMMANDS = {
                "show collections",
                "show dbs", 
                "show databases",
                "db stats", 
                "db.stats()"
            }
            
            SAFE_DICT_OPERATIONS = {
                # Read-only collection operations
                "find", "count", "distinct", "aggregate",
                # Read-only database commands
                "listCollections", "dbstats", "collstats", "dataSize", "dbStats",
                "listIndexes", "getParameter", "buildInfo", "connectionStatus", "serverStatus"
            }
            
            # Check for unsafe keywords that might be part of a query
            def contains_unsafe_operations(cmd_str: str) -> bool:
                UNSAFE_KEYWORDS = [
                    "insert", "update", "delete", "remove", "drop", "create", 
                    "replace", "rename", "$out", "$merge", "mapreduce", 
                    "createindex", "dropindex", "createcollection", "renameCollection"
                ]
                cmd_lower = cmd_str.lower()
                return any(keyword in cmd_lower for keyword in UNSAFE_KEYWORDS)
                    
            # Handle string-based commands
            if isinstance(raw_command, str):
                cmd = raw_command.strip()
                cmd_lower = cmd.lower()
                
                # Block potentially unsafe string commands
                if contains_unsafe_operations(cmd_lower):
                    self.logger.add_log(f"❌ Blocked unsafe operation in command: {raw_command}")
                    return {"error": "Operation blocked - only read operations are permitted"}
                    
                # Process safe standard string commands
                if cmd_lower in SAFE_STRING_COMMANDS:
                    if cmd_lower == "show collections":
                        result = self.db.list_collection_names()
                        return result
                    elif cmd_lower == "show dbs" or cmd_lower == "show databases":
                        result = self.client.list_database_names()
                        return result
                    elif cmd_lower == "db stats" or cmd_lower == "db.stats()":
                        return self.db.command("dbstats")
                
                # Handle MongoDB shell syntax (db.collection.method())
                elif cmd.startswith("db."):
                    # Parse the command to extract collection and operation
                    parts = cmd.split(".")
                    
                    # Handle db.getCollectionNames()
                    if len(parts) == 2 and parts[1] == "getCollectionNames()":
                        return self.db.list_collection_names()
                    
                    # Handle collection operations like db.users.find()
                    elif len(parts) >= 3:
                        collection_name = parts[1]
                        operation_part = ".".join(parts[2:])
                        
                        # Extract the operation name and parameters
                        match = re.match(r'(\w+)\((.*)\)(?:\.(\w+)\((.*)\))?', operation_part)
                        if match:
                            primary_op, primary_params, chained_op, chained_params = match.groups()
                            
                            # Handle findOne()
                            if primary_op == "findOne":
                                filter_dict = {} if not primary_params.strip() else json.loads(primary_params)
                                result = self.db[collection_name].find_one(filter_dict)
                                return json.loads(json_util.dumps(result)) if result else None
                            
                            # Handle find() and variations
                            elif primary_op == "find":
                                filter_dict = {} if not primary_params.strip() else json.loads(primary_params)
                                
                                # Handle chained operations like find().count()
                                if chained_op == "count":
                                    return self.db[collection_name].count_documents(filter_dict)
                                else:
                                    result = list(self.db[collection_name].find(filter_dict))
                                    return json.loads(json_util.dumps(result)) if result else []
                            
                            # Handle aggregate()
                            elif primary_op == "aggregate":
                                # Ensure the pipeline doesn't contain $out or $merge
                                pipeline = json.loads(primary_params)
                                for stage in pipeline:
                                    if "$out" in stage or "$merge" in stage:
                                        self.logger.add_log(f"❌ Blocked unsafe aggregation stage in: {raw_command}")
                                        return {"error": "Unsafe aggregation stage detected"}
                                
                                result = list(self.db[collection_name].aggregate(pipeline))
                                return json.loads(json_util.dumps(result)) if result else []
                            
                            # Handle count()
                            elif primary_op == "count":
                                filter_dict = {} if not primary_params.strip() else json.loads(primary_params)
                                return self.db[collection_name].count_documents(filter_dict)
                            
                            # Handle distinct()
                            elif primary_op == "distinct":
                                params = primary_params.split(",")
                                if len(params) >= 1:
                                    key = params[0].strip().strip('"\'')
                                    filter_dict = {} if len(params) < 2 else json.loads(params[1])
                                    result = self.db[collection_name].distinct(key, filter_dict)
                                    return json.loads(json_util.dumps(result)) if result else []
                
                # Command not recognized
                self.logger.add_log(f"❌ Unsupported or potentially unsafe string command: {raw_command}")
                return {"error": "Command not supported in read-only mode"}

            # Handle dict-based commands
            elif isinstance(raw_command, dict):
                # Convert to string to check for unsafe operations
                cmd_str = json.dumps(raw_command)
                if contains_unsafe_operations(cmd_str):
                    self.logger.add_log(f"❌ Blocked unsafe operation in command: {raw_command}")
                    return {"error": "Operation blocked - only read operations are permitted"}
                
                # Handle find operation explicitly (most common read operation)
                if "find" in raw_command:
                    coll_name = raw_command["find"]
                    filter_ = raw_command.get("filter", {})
                    projection = raw_command.get("projection")
                    sort = raw_command.get("sort")
                    limit = raw_command.get("limit")

                    cursor = self.db[coll_name].find(filter_, projection or {})
                    if sort:
                        cursor = cursor.sort(sort.items() if isinstance(sort, dict) else sort)
                    if limit:
                        cursor = cursor.limit(limit)

                    result = list(cursor)
                    return json.loads(json_util.dumps(result)) if result else []
                
                # Handle other safe operations
                elif any(op in raw_command for op in ["count", "distinct", "aggregate"]):
                    op = next(op for op in ["count", "distinct", "aggregate"] if op in raw_command)
                    coll_name = raw_command[op]
                    
                    if op == "count":
                        filter_ = raw_command.get("filter", {})
                        return self.db[coll_name].count_documents(filter_)
                    
                    elif op == "distinct":
                        key = raw_command.get("key")
                        filter_ = raw_command.get("filter", {})
                        if not key:
                            return {"error": "distinct operation requires a 'key' parameter"}
                        result = self.db[coll_name].distinct(key, filter_)
                        return json.loads(json_util.dumps(result)) if result else []
                    
                    elif op == "aggregate":
                        pipeline = raw_command.get("pipeline", [])
                        # Check for unsafe aggregation stages
                        for stage in pipeline:
                            if any(unsafe_stage in stage for unsafe_stage in ["$out", "$merge"]):
                                self.logger.add_log(f"❌ Blocked unsafe aggregation stage: {stage}")
                                return {"error": "Unsafe aggregation stage detected"}
                        
                        result = list(self.db[coll_name].aggregate(pipeline))
                        return json.loads(json_util.dumps(result)) if result else []
                
                # For other dict commands, only allow specifically whitelisted operations
                else:
                    # Extract operation name (first key in dict)
                    operation = next(iter(raw_command), None)
                    if operation in SAFE_DICT_OPERATIONS:
                        # Run only whitelisted commands
                        return self.db.command(raw_command)
                    else:
                        self.logger.add_log(f"❌ Blocked non-whitelisted operation: {operation}")
                        return {"error": f"Operation '{operation}' not permitted in read-only mode"}

            else:
                self.logger.add_log(f"❌ Invalid command format: {raw_command}")
                return {"error": "Invalid command format"}

        except Exception as e:
            self.logger.add_log(f"❌ Query execution error: {str(e)}")
            return {"error": f"Query execution error: {str(e)}"}

 
    # def execute_query(self, raw_command: Union[str, Dict[str, Any]]) -> Any:
    #     """
    #     Executes a dynamic MongoDB command or query string for READ-ONLY operations.
        
    #     Any commands that would modify the database (create, update, delete) are blocked.

    #     Args:
    #         raw_command (str | dict): The read-only command/query to execute. Can be a string like 'show collections' 
    #                                 or a dict for commands like {'listCollections': 1}.

    #     Returns:
    #         Any: Query results or metadata depending on the command.
    #     """
    #     if self.client is None or self.db is None:
    #         self.logger.add_log("❌ No active MongoDB connection.")
    #         return None

    #     try:
    #         # Define safe operations
    #         SAFE_STRING_COMMANDS = {
    #             "show collections",
    #             "show dbs", 
    #             "show databases",
    #             "db stats", 
    #             "db.stats()"
    #         }
            
    #         SAFE_DICT_OPERATIONS = {
    #             # Read-only collection operations
    #             "find", "count", "distinct", "aggregate",
    #             # Read-only database commands
    #             "listCollections", "dbstats", "collstats", "dataSize", "dbStats",
    #             "listIndexes", "getParameter", "buildInfo", "connectionStatus", "serverStatus"
    #         }
            
    #         # Check for unsafe keywords that might be part of a query
    #         def contains_unsafe_operations(cmd_str: str) -> bool:
    #             UNSAFE_KEYWORDS = [
    #                 "insert", "update", "delete", "remove", "drop", "create", 
    #                 "replace", "rename", "$out", "$merge", "mapreduce", 
    #                 "createindex", "dropindex", "createcollection", "renameCollection"
    #             ]
    #             cmd_lower = cmd_str.lower()
    #             return any(keyword in cmd_lower for keyword in UNSAFE_KEYWORDS)
                    
    #         # Handle string-based commands
    #         if isinstance(raw_command, str):
    #             cmd = raw_command.strip().lower()
                
    #             # Block potentially unsafe string commands
    #             if contains_unsafe_operations(cmd):
    #                 self.logger.add_log(f"❌ Blocked unsafe operation in command: {raw_command}")
    #                 return {"error": "Operation blocked - only read operations are permitted"}
                    
    #             # Process safe string commands
    #             if cmd in SAFE_STRING_COMMANDS:
    #                 if cmd == "show collections":
    #                     result = self.db.list_collection_names()
    #                     return result
    #                 elif cmd == "show dbs" or cmd == "show databases":
    #                     result = self.client.list_database_names()
    #                     return result
    #                 elif cmd == "db stats" or cmd == "db.stats()":
    #                     return self.db.command("dbstats")
    #             else:
    #                 self.logger.add_log(f"❌ Unsupported or potentially unsafe string command: {raw_command}")
    #                 return {"error": "Command not supported in read-only mode"}

    #         # Handle dict-based commands
    #         elif isinstance(raw_command, dict):
    #             # Convert to string to check for unsafe operations
    #             cmd_str = json.dumps(raw_command)
    #             if contains_unsafe_operations(cmd_str):
    #                 self.logger.add_log(f"❌ Blocked unsafe operation in command: {raw_command}")
    #                 return {"error": "Operation blocked - only read operations are permitted"}
                
    #             # Handle find operation explicitly (most common read operation)
    #             if "find" in raw_command:
    #                 coll_name = raw_command["find"]
    #                 filter_ = raw_command.get("filter", {})
    #                 projection = raw_command.get("projection")
    #                 sort = raw_command.get("sort")
    #                 limit = raw_command.get("limit")

    #                 cursor = self.db[coll_name].find(filter_, projection or {})
    #                 if sort:
    #                     cursor = cursor.sort(sort.items() if isinstance(sort, dict) else sort)
    #                 if limit:
    #                     cursor = cursor.limit(limit)

    #                 result = list(cursor)
    #                 return json.loads(json_util.dumps(result)) if result else []
                
    #             # Handle other safe operations
    #             elif any(op in raw_command for op in ["count", "distinct", "aggregate"]):
    #                 op = next(op for op in ["count", "distinct", "aggregate"] if op in raw_command)
    #                 coll_name = raw_command[op]
                    
    #                 if op == "count":
    #                     filter_ = raw_command.get("filter", {})
    #                     return self.db[coll_name].count_documents(filter_)
                    
    #                 elif op == "distinct":
    #                     key = raw_command.get("key")
    #                     filter_ = raw_command.get("filter", {})
    #                     if not key:
    #                         return {"error": "distinct operation requires a 'key' parameter"}
    #                     result = self.db[coll_name].distinct(key, filter_)
    #                     return json.loads(json_util.dumps(result)) if result else []
                    
    #                 elif op == "aggregate":
    #                     pipeline = raw_command.get("pipeline", [])
    #                     # Check for unsafe aggregation stages
    #                     for stage in pipeline:
    #                         if any(unsafe_stage in stage for unsafe_stage in ["$out", "$merge"]):
    #                             self.logger.add_log(f"❌ Blocked unsafe aggregation stage: {stage}")
    #                             return {"error": "Unsafe aggregation stage detected"}
                        
    #                     result = list(self.db[coll_name].aggregate(pipeline))
    #                     return json.loads(json_util.dumps(result)) if result else []
                
    #             # For other dict commands, only allow specifically whitelisted operations
    #             else:
    #                 # Extract operation name (first key in dict)
    #                 operation = next(iter(raw_command), None)
    #                 if operation in SAFE_DICT_OPERATIONS:
    #                     # Run only whitelisted commands
    #                     return self.db.command(raw_command)
    #                 else:
    #                     self.logger.add_log(f"❌ Blocked non-whitelisted operation: {operation}")
    #                     return {"error": f"Operation '{operation}' not permitted in read-only mode"}

    #         else:
    #             self.logger.add_log(f"❌ Invalid command format: {raw_command}")
    #             return {"error": "Invalid command format"}

    #     except Exception as e:
    #         self.logger.add_log(f"❌ Query execution error: {str(e)}")
    #         return {"error": f"Query execution error: {str(e)}"}
    
    def execute_query_to_dataframe(
        self, 
        collection: str, 
        query: Dict[str, Any] = {}, 
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[Union[Dict[str, int], List[tuple]]] = None,
        limit: Optional[int] = None,
        timeout_ms: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        Execute a MongoDB find query and return results as DataFrame.
        
        Args:
            collection (str): Collection name
            query (Dict[str, Any], optional): Query filter
            projection (Dict[str, Any], optional): Fields to include/exclude
            sort (Union[Dict[str, int], List[tuple]], optional): Sort specification
            limit (int, optional): Number of results to return
            timeout_ms (int, optional): Operation timeout in milliseconds
            
        Returns:
            pd.DataFrame: DataFrame containing query results or None if failed
        """
        # return self.execute_query(
        #     collection=collection,
        #     operation="find",
        #     query=query,
        #     projection=projection,
        #     sort=sort,
        #     limit=limit,
        #     timeout_ms=timeout_ms
        # )
        return self.execute_query(query)
    
    def get_collection_schema(self) -> bool:
        """
        Get schema information for all collections in the database.
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.add_log("Retrieving collection schema...")

        if self.client is None or self.db is None:
            self.logger.add_log("MongoDB not connected")
            return False

        try:
            # Get list of collections
            collections = self.db.list_collection_names()
            
            # Get schema information for each collection
            for collection_name in collections:
                # Sample a few documents to infer schema
                sample_docs = list(self.db[collection_name].find().limit(100))
                
                if not sample_docs:
                    self.collection_schema[collection_name] = []
                    continue
                
                # Extract field information from sample documents
                field_info = {}
                for doc in sample_docs:
                    for field, value in doc.items():
                        if field not in field_info:
                            field_info[field] = {
                                "name": field,
                                "type": type(value).__name__,
                                "nullable": True  # MongoDB fields are always nullable
                            }
                
                # Convert to list format similar to the PostgreSQL version
                self.collection_schema[collection_name] = [
                    {"name": name, "type": info["type"], "nullable": info["nullable"]}
                    for name, info in field_info.items()
                ]
                
            self.logger.add_log(f"✅ Retrieved schema for {len(collections)} collections")
            return True
            
        except Exception as e:
            self.logger.add_log(f"❌ Error retrieving collection schema: {e}")
            return False
    
    def get_collection_relationships(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Attempt to infer relationships between collections based on field naming.
        
        Note: MongoDB doesn't enforce relationships like SQL databases.
        This is a best-effort attempt to identify potential references.
        
        Returns:
            Dictionary of collection relationships
        """
        if self.client is None or self.db is None:
            self.logger.add_log("MongoDB not connected")
            return {}
            
        try:
            relationships = {}
            collections = self.db.list_collection_names()
            
            # First, ensure we have schema information
            if not self.collection_schema:
                self.get_collection_schema()
            
            # Look for potential relationships based on field naming patterns
            for collection_name in collections:
                relationships[collection_name] = []
                
                # Skip if no schema info available
                if collection_name not in self.collection_schema:
                    continue
                    
                fields = self.collection_schema[collection_name]
                
                for field in fields:
                    field_name = field["name"]
                    
                    # Common patterns for references
                    if field_name.endswith("_id") and field_name != "_id":
                        # Extract potential referenced collection name
                        referenced_collection = field_name[:-3]
                        
                        # Check for plural form
                        if referenced_collection.endswith("s"):
                            referenced_collection_singular = referenced_collection[:-1]
                        else:
                            referenced_collection_singular = referenced_collection
                            referenced_collection = referenced_collection + "s"
                        
                        # Check if either form exists as a collection
                        if referenced_collection in collections or referenced_collection_singular in collections:
                            target_collection = referenced_collection if referenced_collection in collections else referenced_collection_singular
                            
                            relationships[collection_name].append({
                                "column": field_name,
                                "references_table": target_collection,
                                "references_column": "_id"
                            })
                    
                    # Field with the exact name of another collection
                    elif field_name in collections:
                        relationships[collection_name].append({
                            "column": field_name,
                            "references_table": field_name,
                            "references_column": "unknown"
                        })
                
            self.logger.add_log("Inferred collection relationships successfully")
            return relationships
            
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"❌ Error inferring collection relationships: {error_msg}")
            return {}
            
    def get_collection_info(self) -> Dict[str, Any]:
        """
        Get enriched collection information including inferred relationships and statistics.
        
        Returns:
            Dictionary with comprehensive collection information
        """
        self.logger.add_log("Retrieving collection information...")
        
        try:
            # First, ensure we have schema information
            if not self.collection_schema:
                self.get_collection_schema()
            
            # Get inferred relationships
            relationships = self.get_collection_relationships()
            
            # Get collection statistics and indexes
            collections_info = {}
            
            if self.client and self.db:
                for collection_name in self.db.list_collection_names():
                    collection = self.db[collection_name]
                    
                    # Get basic collection statistics
                    stats = self.db.command("collStats", collection_name)
                    
                    # Get indexes
                    indexes = list(collection.list_indexes())
                    formatted_indexes = []
                    
                    for index in indexes:
                        formatted_indexes.append({
                            "name": index["name"],
                            "keys": index["key"],
                            "unique": index.get("unique", False)
                        })
                    
                    # Sample documents for schema inference
                    sample_docs = list(collection.find().limit(5))
                    samples = json.loads(json_util.dumps(sample_docs))
                    
                    collections_info[collection_name] = {
                        "count": stats.get("count", 0),
                        "size": stats.get("size", 0),
                        "avgObjSize": stats.get("avgObjSize", 0),
                        "storageSize": stats.get("storageSize", 0),
                        "fields": self.collection_schema.get(collection_name, []),
                        "indexes": formatted_indexes,
                        "sample_documents": samples
                    }
            
            schema_info = {
                "database": self.db.name if self.db else "unknown",
                "collections": collections_info,
                "relationships": relationships
            }
            
            self.logger.add_log("Rich collection info retrieved successfully")
            return schema_info
            
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            self.logger.add_log(f"❌ Error retrieving collection info: {error_msg}")
            return {
                "collections": self.collection_schema, 
                "relationships": {}, 
                "error": str(e)
            }
    
    def get_rich_schema_info(self) -> Dict[str, Any]:
        """
        Alias for get_collection_info to maintain API compatibility with PostgresDBManager.
        
        Returns:
            Dictionary with comprehensive collection information
        """
        return self.get_collection_info()
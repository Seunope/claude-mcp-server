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
                - Advanced MongoDB queries with various formats

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
                "db.stats()",
                "show profile",
                "db.getProfilingStatus()",
                "db.version()"
            }
            
            SAFE_DICT_OPERATIONS = {
                # Read-only collection operations
                "find", "count", "distinct", "aggregate", "findOne", "countDocuments", "estimatedDocumentCount",
                # Read-only database commands
                "listCollections", "dbstats", "collstats", "dataSize", "dbStats", "ping", "hostInfo", "serverInfo",
                "listIndexes", "getParameter", "buildInfo", "connectionStatus", "serverStatus", "validate", "profile"
            }
            
            # Check for unsafe keywords that might be part of a query
            def contains_unsafe_operations(cmd_str: str) -> bool:
                UNSAFE_PATTERNS = [
                    r"\binsert\b", r"\bupdate\b", r"\bdelete\b", r"\bremove\b", 
                    r"\bdrop\b", r"\bcreate\b", r"\breplace\b", r"\brename\b",
                    r"\$out\b", r"\$merge\b", r"\bmapreduce\b", 
                    r"\bcreateindex\b", r"\bdropindex\b", r"\bcreatecollection\b", r"\brenameCollection\b"
                ]
                cmd_lower = cmd_str.lower()
                # Special case for operators like $dateToString that contain "safe" words as substrings
                SAFE_OPERATORS = [
                    "$dateToString", "$dateFromString", "$dateToparts", "$createDate"
                ]
                
                # Check if any of the safe operators are in the command
                for safe_op in SAFE_OPERATORS:
                    if safe_op.lower() in cmd_lower:
                        # Remove these safe operators from the string before checking for unsafe patterns
                        cmd_lower = cmd_lower.replace(safe_op.lower(), "")
                        
                # Check for unsafe patterns
                return any(re.search(pattern, cmd_lower) for pattern in UNSAFE_PATTERNS)
            
            # Parse MongoDB shell syntax more robustly
            def parse_shell_command(cmd: str):
                # Extract collection and operation parts
                if not cmd.startswith("db."):
                    return None, None, None, None
                
                parts = cmd.split(".", 2)
                if len(parts) < 3:
                    return None, None, None, None
                
                collection_name = parts[1]
                operation_part = parts[2]
                
                # Handle cases like db.collection.find({...}).sort({...}).limit(10)
                op_chain = []
                current_op = ""
                depth = 0
                param_start = -1
                
                for i, char in enumerate(operation_part):
                    if char == '(' and depth == 0:
                        # Start of parameters
                        op_name = current_op.strip()
                        param_start = i + 1
                        depth += 1
                        current_op = ""
                    elif char == '(' and depth > 0:
                        depth += 1
                    elif char == ')' and depth > 0:
                        depth -= 1
                        if depth == 0:
                            # End of parameters
                            params = operation_part[param_start:i].strip()
                            op_chain.append((op_name, params))
                    elif char == '.' and depth == 0:
                        # Next operation in chain
                        current_op = ""
                    elif depth == 0:
                        current_op += char
                
                if not op_chain:
                    return None, None, None, None
                
                primary_op, primary_params = op_chain[0]
                chained_ops = op_chain[1:] if len(op_chain) > 1 else []
                
                return collection_name, primary_op, primary_params, chained_ops
            
            # Parse and evaluate JSON-like parameters safely
            def safe_eval_params(params_str: str):
                if not params_str.strip():
                    return {}
                    
                # Try to handle various parameter formats
                try:
                    # First attempt direct JSON parsing
                    return json.loads(params_str)
                except json.JSONDecodeError:
                    try:
                        # Handle JavaScript style parameters (unquoted keys)
                        # Convert to proper JSON format by quoting keys
                        fixed_params = re.sub(r'(\w+)(?=\s*:)', r'"\1"', params_str)
                        return json.loads(fixed_params)
                    except (json.JSONDecodeError, re.error):
                        try:
                            # Handle ObjectId references
                            if "ObjectId" in params_str:
                                # Replace ObjectId syntax with proper format
                                params_str = re.sub(r'ObjectId\(["\'](.+?)["\']\)', r'{"$oid": "\1"}', params_str)
                                return json.loads(params_str)
                        except (json.JSONDecodeError, re.error):
                            self.logger.add_log(f"❌ Failed to parse parameters: {params_str}")
                            raise ValueError(f"Could not parse query parameters: {params_str}")
                    
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
                    elif cmd_lower in ("show dbs", "show databases"):
                        result = self.client.list_database_names()
                        return result
                    elif cmd_lower in ("db stats", "db.stats()"):
                        return self.db.command("dbstats")
                    elif cmd_lower == "show profile":
                        return self.db.command("profile", -1)
                    elif cmd_lower == "db.getProfilingStatus()":
                        return self.db.command("profile", -1)
                    elif cmd_lower == "db.version()":
                        return self.db.command("buildInfo").get("version")
                
                # Handle MongoDB shell syntax (db.collection.method())
                elif cmd.startswith("db."):
                    # Parse the command to extract collection and operation
                    collection_name, primary_op, primary_params, chained_ops = parse_shell_command(cmd)
                    
                    if collection_name == "getCollectionNames()":
                        return self.db.list_collection_names()
                    
                    if not collection_name or not primary_op:
                        self.logger.add_log(f"❌ Could not parse MongoDB shell command: {cmd}")
                        return {"error": "Invalid MongoDB shell command format"}
                    
                    # Process the primary operation
                    try:
                        collection = self.db[collection_name]
                        
                        # Handle findOne()
                        if primary_op == "findOne":
                            filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
                            result = collection.find_one(filter_dict)
                            return json.loads(json_util.dumps(result)) if result else None
                        
                        # Handle find() and variations
                        elif primary_op == "find":
                            filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
                            cursor = collection.find(filter_dict)
                            
                            # Process chained operations (sort, limit, skip, etc.)
                            for op_name, op_params in chained_ops:
                                if op_name == "count":
                                    return collection.count_documents(filter_dict)
                                elif op_name == "sort":
                                    sort_params = safe_eval_params(op_params)
                                    cursor = cursor.sort(list(sort_params.items()))
                                elif op_name == "limit":
                                    limit_val = int(op_params.strip())
                                    cursor = cursor.limit(limit_val)
                                elif op_name == "skip":
                                    skip_val = int(op_params.strip())
                                    cursor = cursor.skip(skip_val)
                                elif op_name == "project" or op_name == "projection":
                                    proj_params = safe_eval_params(op_params)
                                    cursor = cursor.projection(proj_params)
                            
                            # If no chained operations consumed the result, return the cursor as a list
                            result = list(cursor)
                            return json.loads(json_util.dumps(result)) if result else []
                        
                        # Handle aggregate()
                        elif primary_op == "aggregate":
                            pipeline = safe_eval_params(primary_params) if primary_params.strip() else []
                            
                            # Ensure the pipeline doesn't contain $out or $merge
                            for stage in pipeline:
                                if "$out" in stage or "$merge" in stage:
                                    self.logger.add_log(f"❌ Blocked unsafe aggregation stage in: {raw_command}")
                                    return {"error": "Unsafe aggregation stage detected"}
                            
                            result = list(collection.aggregate(pipeline))
                            return json.loads(json_util.dumps(result)) if result else []
                        
                        # Handle count() and countDocuments()
                        elif primary_op in ("count", "countDocuments"):
                            filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
                            return collection.count_documents(filter_dict)
                        
                        # Handle estimatedDocumentCount()
                        elif primary_op == "estimatedDocumentCount":
                            return collection.estimated_document_count()
                        
                        # Handle distinct()
                        elif primary_op == "distinct":
                            # Split params by the first comma outside of brackets/objects
                            params = []
                            current_param = ""
                            depth = 0
                            
                            for char in primary_params:
                                if char in "{[":
                                    depth += 1
                                    current_param += char
                                elif char in "}]":
                                    depth -= 1
                                    current_param += char
                                elif char == ',' and depth == 0:
                                    params.append(current_param.strip())
                                    current_param = ""
                                else:
                                    current_param += char
                            
                            if current_param:
                                params.append(current_param.strip())
                            
                            if len(params) >= 1:
                                key = params[0].strip().strip('"\'')
                                filter_dict = {} if len(params) < 2 else safe_eval_params(params[1])
                                result = collection.distinct(key, filter_dict)
                                return json.loads(json_util.dumps(result)) if result else []
                        
                        # Handle stats()
                        elif primary_op == "stats":
                            return self.db.command("collstats", collection_name)
                        
                        # Handle explain()
                        elif primary_op == "explain":
                            explain_params = safe_eval_params(primary_params)
                            return collection.find(explain_params).explain()
                    
                    except Exception as e:
                        self.logger.add_log(f"❌ Error executing MongoDB shell command: {str(e)}")
                        return {"error": f"Error executing command: {str(e)}"}
                
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
                    skip = raw_command.get("skip")
                    hint = raw_command.get("hint")
                    max_time_ms = raw_command.get("maxTimeMS")

                    cursor = self.db[coll_name].find(filter_, projection or {})
                    if sort:
                        cursor = cursor.sort(list(sort.items()) if isinstance(sort, dict) else sort)
                    if limit:
                        cursor = cursor.limit(limit)
                    if skip:
                        cursor = cursor.skip(skip)
                    if hint:
                        cursor = cursor.hint(hint)
                    if max_time_ms:
                        cursor = cursor.max_time_ms(max_time_ms)

                    result = list(cursor)
                    return json.loads(json_util.dumps(result)) if result else []
                
                # Handle findOne operation
                elif "findOne" in raw_command:
                    coll_name = raw_command["findOne"]
                    filter_ = raw_command.get("filter", {})
                    projection = raw_command.get("projection")
                    
                    result = self.db[coll_name].find_one(filter_, projection or {})
                    return json.loads(json_util.dumps(result)) if result else None
                
                # Handle other collection operations
                elif any(op in raw_command for op in ["count", "countDocuments", "distinct", "aggregate"]):
                    op = next(op for op in ["count", "countDocuments", "distinct", "aggregate"] if op in raw_command)
                    coll_name = raw_command[op]
                    
                    if op in ["count", "countDocuments"]:
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
                
                # For database and collection information commands
                elif "listCollections" in raw_command:
                    filter_ = raw_command.get("filter", {})
                    return list(self.db.list_collections(filter_))
                
                elif "collStats" in raw_command or "collstats" in raw_command:
                    coll_name = raw_command.get("collStats") or raw_command.get("collstats")
                    return self.db.command("collstats", coll_name)
                
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
        
    #     Supports both dictionary-based commands and MongoDB shell-style syntax.
    #     Any commands that would modify the database (create, update, delete) are blocked.

    #     Args:
    #         raw_command (str | dict): The read-only command/query to execute. Can be:
    #             - String like 'show collections'
    #             - MongoDB shell syntax like 'db.users.find()'
    #             - Dict for commands like {'find': 'users', 'filter': {...}}
    #             - Advanced MongoDB queries with various formats

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
    #             "db.stats()",
    #             "show profile",
    #             "db.getProfilingStatus()",
    #             "db.version()"
    #         }
            
    #         SAFE_DICT_OPERATIONS = {
    #             # Read-only collection operations
    #             "find", "count", "distinct", "aggregate", "findOne", "countDocuments", "estimatedDocumentCount",
    #             # Read-only database commands
    #             "listCollections", "dbstats", "collstats", "dataSize", "dbStats", "ping", "hostInfo", "serverInfo",
    #             "listIndexes", "getParameter", "buildInfo", "connectionStatus", "serverStatus", "validate", "profile"
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
            
    #         # Parse MongoDB shell syntax more robustly
    #         def parse_shell_command(cmd: str):
    #             # Extract collection and operation parts
    #             if not cmd.startswith("db."):
    #                 return None, None, None, None
                
    #             parts = cmd.split(".", 2)
    #             if len(parts) < 3:
    #                 return None, None, None, None
                
    #             collection_name = parts[1]
    #             operation_part = parts[2]
                
    #             # Handle cases like db.collection.find({...}).sort({...}).limit(10)
    #             op_chain = []
    #             current_op = ""
    #             depth = 0
    #             param_start = -1
                
    #             for i, char in enumerate(operation_part):
    #                 if char == '(' and depth == 0:
    #                     # Start of parameters
    #                     op_name = current_op.strip()
    #                     param_start = i + 1
    #                     depth += 1
    #                     current_op = ""
    #                 elif char == '(' and depth > 0:
    #                     depth += 1
    #                 elif char == ')' and depth > 0:
    #                     depth -= 1
    #                     if depth == 0:
    #                         # End of parameters
    #                         params = operation_part[param_start:i].strip()
    #                         op_chain.append((op_name, params))
    #                 elif char == '.' and depth == 0:
    #                     # Next operation in chain
    #                     current_op = ""
    #                 elif depth == 0:
    #                     current_op += char
                
    #             if not op_chain:
    #                 return None, None, None, None
                
    #             primary_op, primary_params = op_chain[0]
    #             chained_ops = op_chain[1:] if len(op_chain) > 1 else []
                
    #             return collection_name, primary_op, primary_params, chained_ops
            
    #         # Parse and evaluate JSON-like parameters safely
    #         def safe_eval_params(params_str: str):
    #             if not params_str.strip():
    #                 return {}
                    
    #             # Try to handle various parameter formats
    #             try:
    #                 # First attempt direct JSON parsing
    #                 return json.loads(params_str)
    #             except json.JSONDecodeError:
    #                 try:
    #                     # Handle JavaScript style parameters (unquoted keys)
    #                     # Convert to proper JSON format by quoting keys
    #                     fixed_params = re.sub(r'(\w+)(?=\s*:)', r'"\1"', params_str)
    #                     return json.loads(fixed_params)
    #                 except (json.JSONDecodeError, re.error):
    #                     try:
    #                         # Handle ObjectId references
    #                         if "ObjectId" in params_str:
    #                             # Replace ObjectId syntax with proper format
    #                             params_str = re.sub(r'ObjectId\(["\'](.+?)["\']\)', r'{"$oid": "\1"}', params_str)
    #                             return json.loads(params_str)
    #                     except (json.JSONDecodeError, re.error):
    #                         self.logger.add_log(f"❌ Failed to parse parameters: {params_str}")
    #                         raise ValueError(f"Could not parse query parameters: {params_str}")
                    
    #         # Handle string-based commands
    #         if isinstance(raw_command, str):
    #             cmd = raw_command.strip()
    #             cmd_lower = cmd.lower()
                
    #             # Block potentially unsafe string commands
    #             if contains_unsafe_operations(cmd_lower):
    #                 self.logger.add_log(f"❌ Blocked unsafe operation in command: {raw_command}")
    #                 return {"error": "Operation blocked - only read operations are permitted"}
                    
    #             # Process safe standard string commands
    #             if cmd_lower in SAFE_STRING_COMMANDS:
    #                 if cmd_lower == "show collections":
    #                     result = self.db.list_collection_names()
    #                     return result
    #                 elif cmd_lower in ("show dbs", "show databases"):
    #                     result = self.client.list_database_names()
    #                     return result
    #                 elif cmd_lower in ("db stats", "db.stats()"):
    #                     return self.db.command("dbstats")
    #                 elif cmd_lower == "show profile":
    #                     return self.db.command("profile", -1)
    #                 elif cmd_lower == "db.getProfilingStatus()":
    #                     return self.db.command("profile", -1)
    #                 elif cmd_lower == "db.version()":
    #                     return self.db.command("buildInfo").get("version")
                
    #             # Handle MongoDB shell syntax (db.collection.method())
    #             elif cmd.startswith("db."):
    #                 # Parse the command to extract collection and operation
    #                 collection_name, primary_op, primary_params, chained_ops = parse_shell_command(cmd)
                    
    #                 if collection_name == "getCollectionNames()":
    #                     return self.db.list_collection_names()
                    
    #                 if not collection_name or not primary_op:
    #                     self.logger.add_log(f"❌ Could not parse MongoDB shell command: {cmd}")
    #                     return {"error": "Invalid MongoDB shell command format"}
                    
    #                 # Process the primary operation
    #                 try:
    #                     collection = self.db[collection_name]
                        
    #                     # Handle findOne()
    #                     if primary_op == "findOne":
    #                         filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
    #                         result = collection.find_one(filter_dict)
    #                         return json.loads(json_util.dumps(result)) if result else None
                        
    #                     # Handle find() and variations
    #                     elif primary_op == "find":
    #                         filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
    #                         cursor = collection.find(filter_dict)
                            
    #                         # Process chained operations (sort, limit, skip, etc.)
    #                         for op_name, op_params in chained_ops:
    #                             if op_name == "count":
    #                                 return collection.count_documents(filter_dict)
    #                             elif op_name == "sort":
    #                                 sort_params = safe_eval_params(op_params)
    #                                 cursor = cursor.sort(list(sort_params.items()))
    #                             elif op_name == "limit":
    #                                 limit_val = int(op_params.strip())
    #                                 cursor = cursor.limit(limit_val)
    #                             elif op_name == "skip":
    #                                 skip_val = int(op_params.strip())
    #                                 cursor = cursor.skip(skip_val)
    #                             elif op_name == "project" or op_name == "projection":
    #                                 proj_params = safe_eval_params(op_params)
    #                                 cursor = cursor.projection(proj_params)
                            
    #                         # If no chained operations consumed the result, return the cursor as a list
    #                         result = list(cursor)
    #                         return json.loads(json_util.dumps(result)) if result else []
                        
    #                     # Handle aggregate()
    #                     elif primary_op == "aggregate":
    #                         pipeline = safe_eval_params(primary_params) if primary_params.strip() else []
                            
    #                         # Ensure the pipeline doesn't contain $out or $merge
    #                         for stage in pipeline:
    #                             if "$out" in stage or "$merge" in stage:
    #                                 self.logger.add_log(f"❌ Blocked unsafe aggregation stage in: {raw_command}")
    #                                 return {"error": "Unsafe aggregation stage detected"}
                            
    #                         result = list(collection.aggregate(pipeline))
    #                         return json.loads(json_util.dumps(result)) if result else []
                        
    #                     # Handle count() and countDocuments()
    #                     elif primary_op in ("count", "countDocuments"):
    #                         filter_dict = {} if not primary_params.strip() else safe_eval_params(primary_params)
    #                         return collection.count_documents(filter_dict)
                        
    #                     # Handle estimatedDocumentCount()
    #                     elif primary_op == "estimatedDocumentCount":
    #                         return collection.estimated_document_count()
                        
    #                     # Handle distinct()
    #                     elif primary_op == "distinct":
    #                         # Split params by the first comma outside of brackets/objects
    #                         params = []
    #                         current_param = ""
    #                         depth = 0
                            
    #                         for char in primary_params:
    #                             if char in "{[":
    #                                 depth += 1
    #                                 current_param += char
    #                             elif char in "}]":
    #                                 depth -= 1
    #                                 current_param += char
    #                             elif char == ',' and depth == 0:
    #                                 params.append(current_param.strip())
    #                                 current_param = ""
    #                             else:
    #                                 current_param += char
                            
    #                         if current_param:
    #                             params.append(current_param.strip())
                            
    #                         if len(params) >= 1:
    #                             key = params[0].strip().strip('"\'')
    #                             filter_dict = {} if len(params) < 2 else safe_eval_params(params[1])
    #                             result = collection.distinct(key, filter_dict)
    #                             return json.loads(json_util.dumps(result)) if result else []
                        
    #                     # Handle stats()
    #                     elif primary_op == "stats":
    #                         return self.db.command("collstats", collection_name)
                        
    #                     # Handle explain()
    #                     elif primary_op == "explain":
    #                         explain_params = safe_eval_params(primary_params)
    #                         return collection.find(explain_params).explain()
                    
    #                 except Exception as e:
    #                     self.logger.add_log(f"❌ Error executing MongoDB shell command: {str(e)}")
    #                     return {"error": f"Error executing command: {str(e)}"}
                
    #             # Command not recognized
    #             self.logger.add_log(f"❌ Unsupported or potentially unsafe string command: {raw_command}")
    #             return {"error": "Command not supported in read-only mode"}

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
    #                 skip = raw_command.get("skip")
    #                 hint = raw_command.get("hint")
    #                 max_time_ms = raw_command.get("maxTimeMS")

    #                 cursor = self.db[coll_name].find(filter_, projection or {})
    #                 if sort:
    #                     cursor = cursor.sort(list(sort.items()) if isinstance(sort, dict) else sort)
    #                 if limit:
    #                     cursor = cursor.limit(limit)
    #                 if skip:
    #                     cursor = cursor.skip(skip)
    #                 if hint:
    #                     cursor = cursor.hint(hint)
    #                 if max_time_ms:
    #                     cursor = cursor.max_time_ms(max_time_ms)

    #                 result = list(cursor)
    #                 return json.loads(json_util.dumps(result)) if result else []
                
    #             # Handle findOne operation
    #             elif "findOne" in raw_command:
    #                 coll_name = raw_command["findOne"]
    #                 filter_ = raw_command.get("filter", {})
    #                 projection = raw_command.get("projection")
                    
    #                 result = self.db[coll_name].find_one(filter_, projection or {})
    #                 return json.loads(json_util.dumps(result)) if result else None
                
    #             # Handle other collection operations
    #             elif any(op in raw_command for op in ["count", "countDocuments", "distinct", "aggregate"]):
    #                 op = next(op for op in ["count", "countDocuments", "distinct", "aggregate"] if op in raw_command)
    #                 coll_name = raw_command[op]
                    
    #                 if op in ["count", "countDocuments"]:
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
                
    #             # For database and collection information commands
    #             elif "listCollections" in raw_command:
    #                 filter_ = raw_command.get("filter", {})
    #                 return list(self.db.list_collections(filter_))
                
    #             elif "collStats" in raw_command or "collstats" in raw_command:
    #                 coll_name = raw_command.get("collStats") or raw_command.get("collstats")
    #                 return self.db.command("collstats", coll_name)
                
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
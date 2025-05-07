import os
import sys
from src.logger import Logger
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from src.llm.openai_client import OpenAIClient
from src.db.postgres.database import PostgresDBManager
from src.db.postgres.analyzer import PostgresDBAnalyzer
from src.db.mongo.database import MongoDBManager
from src.db.mongo.analyzer import MongoDBAnalyzer
# print("Python executable:", sys.executable, file=sys.stderr)
# print("Python path:", sys.path, file=sys.stderr)
# print("Environment:", os.environ.get('VIRTUAL_ENV'), file=sys.stderr)
# Create an MCP server
mcp = FastMCP("Basic MCP Server")

# Initialize the logger and database manager
logger = Logger()
db_manager = PostgresDBManager(logger)
db_manager2 = MongoDBManager(logger)

# Register MCP tools for Logger
# @mcp.tool()
# def db_analyzer_progress(request: str) -> str:
#     """
#     Analyzer and perform action on progress database base on prompt or request.

#     Returns:
#         str: Confirmation message
#     """

#     analyzer = PostgresDBAnalyzer()
#      # Initialize connections
#     if not analyzer.initialize():
#         logger.add_log("Exiting due to initialization failure. Check logs")
#         sys.exit(1)
    
#         result = analyzer.process_request(request)
        
#         if not result["success"]:
#             logger.add_log(f"❌ Error: {result['error']}")
#             sys.exit(1)

#         logger.add_log('was grt')   
#         # Save results if requested
#         if args.save:
#             filename = save_analysis_to_file(result, args.request)
#             print(f"✅ Analysis saved to {filename}")
        
#         # Send to Claude
#         mcp_response = analyzer.send_to_claude(result)
        
#         if mcp_response and mcp_response.get("success"):
#             print(f"✅ Analysis sent to Claude MCP (Conversation ID: {mcp_response.get('conversation_id')})")
#         else:
#             # If MCP failed, print the analysis
#             if result["sql_query"]:
#                 print("\nSQL Query:")
#                 print(result["sql_query"])
            
#             if result["analysis"]:
#                 print("\nAnalysis:")
#                 print(result["analysis"])
            
#             if result["sample_data"]:
#                 print("\nSample Data:")
#                 print(result["sample_data"])
#     else:
#         print("Either --interactive or --request must be specified")

# @mcp.tool()
# def db_analyzer_mongo(request: str) -> str:
#     """
#     Analyzer and perform action on Mongo database base on prompt or request.

#     Returns:
#         str: Confirmation message
#     """

#     analyzer = MongoDBAnalyzer()
#      # Initialize connections
#     if not analyzer.initialize():
#         logger.add_log("Exiting due to initialization failure. Check logs")
#         sys.exit(1)
    
#         result = analyzer.process_request(request)
        
#         if not result["success"]:
#             logger.add_log(f"❌ Error: {result['error']}")
#             sys.exit(1)

#         return result
#         # logger.add_log('was grt')   
#         # Save results if requested
#         # if args.save:
#         #     filename = save_analysis_to_file(result, args.request)
#         #     print(f"✅ Analysis saved to {filename}")
        
#         # # Send to Claude
#         # mcp_response = analyzer.send_to_claude(result)
        
#         # if mcp_response and mcp_response.get("success"):
#         #     print(f"✅ Analysis sent to Claude MCP (Conversation ID: {mcp_response.get('conversation_id')})")
#         # else:
#         #     # If MCP failed, print the analysis
#         #     if result["sql_query"]:
#         #         print("\nSQL Query:")
#         #         print(result["sql_query"])
            
#         #     if result["analysis"]:
#         #         print("\nAnalysis:")
#         #         print(result["analysis"])
            
#         #     if result["sample_data"]:
#         #         print("\nSample Data:")
#         #         print(result["sample_data"])
#     else:
#         print("Either --interactive or --request must be specified")

# @mcp.tool()
# def db_analyzer_get_schema(type:str) -> str:
#     """
#     Analyzer and get database schema for Mongo and Postgres Db. Type can me "mongo" or "postgres"

#     Returns:
#         str: Confirmation message
#     """

#     if type =='mongo':
#         db_analyzer = MongoDBAnalyzer()
#     else:
#         db_analyzer = PostgresDBAnalyzer()
#     if not db_analyzer.initialize():
#         return "Database or OpenAI connection failed. Check logs for details."
    

#     if type =='mongo': 
#         schema = db_analyzer.db_manager.get_collection_schema() 
#     else:  
#         schema = db_analyzer.db_manager.get_table_schema()
#     if schema is None:
#         return "Failed to retrieve schema. Check logs for details."

#     return 'Schema retrieved check log'

@mcp.tool()
def add_log(message: str) -> str:
    """
    Append a new log to the file.

    Args:
        message (str): The log content to be added.

    Returns:
        str: Confirmation message indicating the log was saved.
    """
    return logger.add_log(message)

@mcp.tool()
def get_logs() -> str:
    """
    Read and return all logs from the log file.

    Returns:
        str: All logs as a single string separated by line breaks.
             If no logs exist, a default message is returned.
    """
    return logger.get_logs()

# Register MCP tools for PostgresDBManager
@mcp.tool()
def connect_to_database() -> str:
    """
    Connect to PostgreSQL database.

    Args:
        host (str): Database host
        port (int): Database port
        dbname (str): Database name
        user (str): Database user
        password (str): Database password

    Returns:
        str: Success or failure message
    """
    load_dotenv()

    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    dbname = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    success = db_manager.connect(host, port, dbname, user, password)
    return "Database connection successful" if success else "Database connection failed"

@mcp.tool()
def disconnect_from_database() -> str:
    """
    Disconnect from the database.

    Returns:
        str: Confirmation message
    """
    db_manager.disconnect()
    return "Database disconnected"

@mcp.tool()
def run_query(query: str, type:str) -> str:
    """
    Run a SQL or Mongo db query on the connected database. Type is either postgres or mongo.

    Args:
        query (str): query to execute

    Returns:
        str: Query results or error message
    """

    if type =='mongo':
        results = db_manager2.execute_query(query)
    else:
        results = db_manager.execute_query(query)
    
    if results is None:
        return "Query execution failed. Check logs for details."
    return f"Query executed successfully. Results: {results}"

@mcp.resource("logs://latest")
def get_latest_log() -> str:
    """
    Get the most recently added log from the log file.

    Returns:
        str: The last log entry. If no logs exist, a default message is returned.
    """
    return logger.get_latest_log()

@mcp.prompt()
def log_summary_prompt() -> str:
    """
    Generate a prompt asking the AI to summarize all current logs.

    Returns:
        str: A prompt string that includes all logs and asks for a summary.
             If no logs exist, a message will be shown indicating that.
    """
    logs = logger.get_logs()
    if logs == "No logs yet.":
        return "There are no logs yet."

    return f"Summarize the current logs: {logs}"

@mcp.tool()
def chat_llm(system_message: str, user_message: str) -> str:
    """
    Run a chat prompt with Open AI

    Args:
        query (str): Chat prompt

    Returns:
        str: Result from chat prompt
    """
    openai = OpenAIClient()
    if not openai.initialize():
        return "OpenAI connection failed. Check logs for details."
    
    results = openai.generate_completion(system_message, user_message)
    if results is None:
        return "Chat completion failed. Check logs for details."
    return f"Chat executed successfully. Response: {results}"


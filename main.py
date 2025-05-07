import os
import sys
from typing import List, Optional
import requests
from src.logger import Logger
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from src.llm.openai_client import OpenAIClient
from src.db.postgres.database import PostgresDBManager
from src.db.postgres.analyzer import PostgresDBAnalyzer
from src.db.mongo.database import MongoDBManager
from src.db.mongo.analyzer import MongoDBAnalyzer
from src.validator import EmailPayload, PushPayload, SMSPayload
# print("Python executable:", sys.executable, file=sys.stderr)
# print("Python path:", sys.path, file=sys.stderr)
# print("Environment:", os.environ.get('VIRTUAL_ENV'), file=sys.stderr)
# Create an MCP server
mcp = FastMCP("Basic MCP Server")

# Initialize the logger and database manager
logger = Logger()
db_manager = PostgresDBManager(logger)
db_manager2 = MongoDBManager(logger)
groupay_base_url= os.environ.get("GROUPAY_BASE_URL")


# Register MCP tools for Logger
@mcp.tool()
def db_analyzer(request: str, type:str) -> str:
    """
    The tool analyzer Progress and Mongo database base on prompt or request.

    Args:
        request (str): Question whose answer can be gotten from database
        type (str) is either postgres or mongo.

    Returns:
        str: Query results or error message
    """
    result = None
    if type =='mongo':
        analyzer = MongoDBAnalyzer()
        if not analyzer.initialize():
            logger.add_log("Exiting due to initialization failure. Check logs")
            sys.exit(1)
        result = analyzer.process_request(request)

    else:
        analyzer = PostgresDBAnalyzer()
        if not analyzer.initialize():
            logger.add_log("Exiting due to initialization failure. Check logs")
            sys.exit(1)
        result = analyzer.process_request(request)
    
    if result is None:
        return "Request execution failed. Check logs for details."
    return f"Request executed successfully. Results: {result}"

@mcp.tool()
def add_log(message: str) -> str:
    """
    Append a new log to the file.

    Args:
        message (str): The log content to be added.

    Returns:
        str: Confirmation message indicating the log was saved.
    """
    logger.add_log(message)
    return message

@mcp.tool()
def get_logs() -> str:
    """
    Read and return all logs from the log file.

    Returns:
        str: All logs as a single string separated by line breaks.
             If no logs exist, a default message is returned.
    """
    return logger.get_logs()


@mcp.tool()
def run_query(query: str, type:str) -> str:
    """
    Run a SQL or NoSQL query on the connected database.

    Args:
        query (str): SQL or NoSQL query to execute
        type (str) is either postgres or mongo.


    Returns:
        str: Query results or error message
    """

    if type =='mongo':
        analyzer = MongoDBAnalyzer()
        if not analyzer.initialize():
            logger.add_log("Exiting due to initialization failure. Check logs")
            sys.exit(1)
        results = analyzer.db_manager.execute_query(query)
        # results = analyzer.db_manager.get_collection_schema()

    else:
        analyzer = PostgresDBAnalyzer()
        if not analyzer.initialize():
            logger.add_log("Exiting due to initialization failure. Check logs")
            sys.exit(1)
        results = analyzer.db_manager.execute_query(query)
    
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
        system_message (str): System message prompt
        user_message (str): User message prompt

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


@mcp.tool()
def send_email(subject: str, message1: str, email: str, message2: Optional[str] = None) -> str:
    """
    Send an email via the notification service.

    Args:
        subject (str): Subject of the email.
        message1 (str): First part of the email message.
        email (str): Recipient's email address.
        message2 (Optional[str]): Second part of the email message (optional).
        name (Optional[str]): Email receiver name (optional).

    Returns:
        str: Response from the email service.
    """
    try:
        payload = EmailPayload(subject=subject, message1=message1, message2=message2, email=email)
        response = requests.post(
            f"{groupay_base_url}/send-email-2",
            json=payload.model_dump(exclude_none=True)
        )
        response.raise_for_status()
        msg = f"Email sent successfully. Response: {response.text}"
        logger.add_log(msg)
        return msg
    except Exception as e:
        msg= f"Failed to send email. Error: {e}"
        logger.add_log(msg)
        return msg

@mcp.tool()
def send_sms(message: str, phone_number: str) -> str:
    """
    Send an SMS via the notification service.

    Args:
        message (str): The SMS message content.
        phone_number (str): Recipient's phone number.

    Returns:
        str: Response from the SMS service.
    """
    try:
        payload = SMSPayload(message=message, phoneNumber=phone_number)
        response = requests.post(f"{groupay_base_url}/send-sms-2",
            json=payload.model_dump(exclude_none=True)          )
        response.raise_for_status()
        msg =  f"SMS sent successfully. Response: {response.text}"
        logger.add_log(msg)
        return msg
    except Exception as e:
        msg= f"Failed to send SMS. Error: {e}"
        logger.add_log(msg)
        return msg

@mcp.tool()
def send_push_notification(message: str, one_signal_ids: List[str], action_name: str) -> str:
    """
    Send a push notification via the notification service.

    Args:
        message (str): The notification message content.
        one_signal_ids (List[str]): List of OneSignal recipient IDs.
        action_name (str): Action name associated with the notification.

    Returns:
        str: Response from the push notification service.
    """
    try:
        payload = PushPayload(message=message, oneSignalIds=one_signal_ids, actionName=action_name)
        response = requests.post(
             f"{groupay_base_url}/push",
            json=payload.model_dump(exclude_none=True)
        )
        response.raise_for_status()
        msg = f"Push notification sent successfully. Response: {response.text}"
        logger.add_log(msg)
        return msg
    except Exception as e:
        msg=  f"Failed to send push notification. Error: {e}"
        logger.add_log(msg)
        return msg

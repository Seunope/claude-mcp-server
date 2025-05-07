"""
Core analyzer functionality for database analysis with LLMs.
"""
import os
import json
import pandas as pd
from src.logger import Logger
from dotenv import load_dotenv
from typing import Dict, Optional, Any

from ...utils import format_markdown_table
from .database import PostgresDBManager
from ...llm.openai_client import OpenAIClient
from ...visualization import create_visualization
load_dotenv()

class PostgresDBAnalyzer:
    """Database analyzer that uses LLMs to translate natural language to SQL and analyze results."""
    
    def __init__(
        self, 
        # db_config: Dict[str, Any], 
        # openai_api_key: Optional[str] = None,
        openai_model: str = "gpt-4o-mini",
        # mcp_api_key: Optional[str] = None
    ):
        """
        Initialize the DB Analyzer.
        
        Args:
            db_config: Database connection parameters
            openai_api_key: OpenAI API key
            openai_model: OpenAI model to use
            mcp_api_key: MCP API key
        """

        logger = Logger()
        openai_api_key= os.environ.get("OPENAI_API_KEY")
        self.db_manager = PostgresDBManager(logger)
       
        self.openai_client = OpenAIClient(api_key=openai_api_key, model=openai_model)
        # self.mcp_client = MCPClient(api_key=mcp_api_key)
        
    def initialize(self) -> bool:
        """
        Initialize connections to database and APIs.
        
        Returns:
            bool: True if all connections successful, False otherwise
        """


        host = os.environ.get("DB_HOST")
        port = os.environ.get("DB_PORT")
        dbname = os.environ.get("DB_NAME")
        user = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASSWORD")

        db_success = self.db_manager.connect(host, port, dbname, user, password)
        openai_success = self.openai_client.initialize()
        # mcp_success = self.mcp_client.initialize()

            
        return db_success and openai_success # and mcp_success

    def translate_to_sql(self, request: str) -> Optional[str]:
        """
        Translate natural language request to a safe SQL query using LLM.
        
        Args:
            request: Natural language request
            
        Returns:
            SQL query string or None if translation failed or unsafe
        """
        schema_info = self.db_manager.get_rich_schema_info()
        schema_context = json.dumps(schema_info, indent=2)

        # Safer system instruction to avoid mutations
        system_message = (
            "You are an expert at translating natural language to SQL queries. "
            "Only return the SQL query without any explanations, comments, or markdown formatting. "
            "Only generate safe, read-only SELECT queries. Never include INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, or any other write operations."
        )

        prompt = f"""
        Here is the database schema:
        ```json
        {schema_context}
        ```

        Natural language request: "{request}"

        Your task is to write a PostgreSQL query that will answer this request.
        Return ONLY the SQL query without any explanation or markdown formatting.
        The query should be complete, correct, and optimized. 
        Do not include any explanations or text outside the SQL query.
        The query must be a safe, read-only SELECT query.
        """

        sql_query = self.openai_client.generate_completion(
            system_message=system_message,
            user_message=prompt,
            temperature=0
        )

        if sql_query:
            cleaned_query = sql_query.strip().lower()
            forbidden_keywords = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]

            if any(keyword in cleaned_query for keyword in forbidden_keywords):
                print("❌ Unsafe SQL query detected. Aborting.")
                return None

            print("✅ Generated safe SQL query from natural language request")
            return sql_query.strip()

        print("❌ Failed to generate SQL query")

        return None


    def analyze_data(self, data: pd.DataFrame, request: str) -> Optional[str]:
        """
        Generate insights from query results using LLM.
        
        Args:
            data: DataFrame with query results
            request: Original natural language request
            
        Returns:
            Analysis text or None if analysis failed
        """
        if data is None or data.empty:
            return "No data available for analysis."
            
        # Convert data to string representation
        data_description = data.head(100).to_string()
        if len(data) > 100:
            data_description += f"\n\n[Note: showing only first 100 rows of {len(data)} total rows]"
            
        data_stats = data.describe().to_string()
            
        system_message = """You are an expert data analyst specializing in database analysis. 
        Provide insightful, actionable analysis based on the data. Format your response in well-structured markdown."""
        
        prompt = f"""
        You've been provided with data from a database query. 
        
        The user's request was: "{request}"
        
        Here's a sample of the data:
        ```
        {data_description}
        ```
        
        Statistical summary:
        ```
        {data_stats}
        ```
        
        Please provide a detailed analysis addressing the user's request. Include:
        1. Key insights and findings
        2. Trends or patterns identified
        3. Actionable recommendations based on the data
        4. Any anomalies or important observations
        
        Your analysis should be thorough yet concise, with clear sections and bullet points where appropriate.
        Format the response in markdown with proper headings and structure.
        """
        
        analysis = self.openai_client.generate_completion(
            system_message=system_message,
            user_message=prompt,
            temperature=0.2
        )
        
        if analysis:
            print("✅ Generated data analysis")
            return analysis
        
        return None
    
    def should_visualize(self, request: str) -> bool:
        """
        Determine if visualization should be generated based on the request.
        
        Args:
            request: Natural language request
            
        Returns:
            True if visualization should be generated
        """
        visualization_keywords = [
            'chart', 'graph', 'plot', 'visualize', 'visualization', 
            'distribution', 'trend', 'compare', 'show me'
        ]
        return any(keyword in request.lower() for keyword in visualization_keywords)
    
    def process_request(self, request: str) -> Dict[str, Any]:
        """
        Process a natural language request end-to-end.
        
        Args:
            request: Natural language request
            
        Returns:
            Dictionary with analysis results
        """
        print(f"🔍 Processing request: {request}")
        
        result = {
            "request": request,
            "success": False,
            "analysis": None,
            "visualization": None,
            "sample_data": None,
            "sql_query": None,
            "error": None
        }
        
        # Generate SQL from request
        sql_query = self.translate_to_sql(request)
        if not sql_query:
            result["error"] = "Failed to translate your request to SQL. Please try rephrasing or provide more details."
            return result
        
        result["sql_query"] = sql_query
        print(f"🔍 Generated SQL query: {sql_query}")
        
        # Execute the query
        data = self.db_manager.execute_query(sql_query)
        if data is None or data.empty:
            result["error"] = "No data found matching your request. Please try a different query."
            return result
            
        print(f"✅ Retrieved {len(data)} rows of data")
        
        # Generate analysis
        analysis = self.analyze_data(data, request)
        if not analysis:
            result["error"] = "Failed to generate analysis from the data."
            return result
        
        result["analysis"] = analysis
        
        # Check if we should generate a visualization
        if self.should_visualize(request):
            try:
                visualization = create_visualization(data, request)
                if visualization:
                    result["visualization"] = visualization
            except Exception as e:
                print(f"⚠️ Warning: Failed to generate visualization: {e}")
        
        # Include sample data
        sample_data = data.head(10) if len(data) > 10 else data
        result["sample_data"] = format_markdown_table(sample_data)
        
        result["success"] = True
        return result
    
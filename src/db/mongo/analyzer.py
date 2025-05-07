import os
import json
import pandas as pd
from src.logger import Logger
from dotenv import load_dotenv
from .database import MongoDBManager
from ...utils import format_markdown_table
from ...llm.openai_client import OpenAIClient
from typing import Dict, Optional, Any, List, Tuple
from ...visualization import create_visualization
load_dotenv()

class MongoDBAnalyzer:
    """MongoDB analyzer that uses LLMs to translate natural language to MongoDB queries and analyze results."""
    
    def __init__(
        self, 
        openai_model: str = "gpt-4o-mini",
    ):
        """
        Initialize the MongoDB Analyzer.
        
        Args:
            openai_model: OpenAI model to use
        """

        logger = Logger()
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        self.db_manager = MongoDBManager(logger)
        self.logger = logger
       
        self.openai_client = OpenAIClient(api_key=openai_api_key, model=openai_model)
        # self.mcp_client = MCPClient(api_key=mcp_api_key)
        
    def initialize(self) -> bool:
        """
        Initialize connections to database and APIs.
        
        Returns:
            bool: True if all connections successful, False otherwise
        """
        connection_string = os.environ.get("MONGODB_CONNECTION_STRING")
        db_name = os.environ.get("MONGODB_DATABASE")

        db_success = self.db_manager.connect(connection_string, db_name)
        openai_success = self.openai_client.initialize()
        # mcp_success = self.mcp_client.initialize()
            
        return db_success and openai_success # and mcp_success
    

    def translate_to_mongodb_query(self, request: str) -> Optional[Dict[str, Any]]:
        """
        Translate natural language request to MongoDB query using LLM.
        Only allows read-only operations (find, aggregate, count, distinct).
        
        Args:
            request: Natural language request
            
        Returns:
            MongoDB query dictionary or None if translation failed or unsafe operation detected
        """
        schema_info = self.db_manager.get_collection_info()
        schema_context = json.dumps(schema_info, indent=2)
        
        # Enhanced system message to emphasize read-only operations
        system_message = """You are an expert at translating natural language to MongoDB READ-ONLY queries. 
        Only return the MongoDB query in a valid JSON format without any explanations, comments or markdown formatting.
        The query should include both the collection name and the query parameters.
        IMPORTANT: You must ONLY generate READ-ONLY queries (find, aggregate, count, distinct).
        Never generate queries that modify the database (insert, update, delete, remove, replaceOne, updateOne, etc)."""
        
        prompt = f"""
        Here is the MongoDB database schema information:
        ```json
        {schema_context}
        ```
        
        Natural language request: "{request}"
        
        Your task is to write a MongoDB READ-ONLY query that will answer this request.
        Return ONLY the MongoDB query as a JSON object with these fields:
        1. "collection": the name of the collection to query
        2. "operation": the type of operation (MUST be one of: find, aggregate, count, distinct)
        3. "query": the query filter parameters
        4. "projection": fields to include/exclude (optional)
        5. "sort": sort specification (optional)
        6. "limit": number of results to return (optional)
        
        Do not include any explanations or text outside the JSON object.
        NEVER generate any operations that would modify the database (insert, update, delete, etc).
        If the request implies a modification operation, return a query that would show the relevant data instead.
        """
        
        query_json = self.openai_client.generate_completion(
            system_message=system_message,
            user_message=prompt,
            temperature=0
        )
        
        if query_json:
            try:
                query_dict = json.loads(query_json.strip())
                
                # Explicit whitelist of safe operations
                SAFE_OPERATIONS = ["find", "aggregate", "count", "distinct"]
                
                # Check if the operation is one of the safe operations
                if "operation" not in query_dict or query_dict["operation"] not in SAFE_OPERATIONS:
                    self.logger.add_log("❌ Unsafe or missing MongoDB operation detected. Aborting.")
                    return None
                    
                # Look for any keywords that might indicate modification operations in the query or aggregation
                query_str = json.dumps(query_dict).lower()
                unsafe_keywords = ["$out", "$merge", "insert", "update", "delete", "remove", "replace", 
                                "createindex", "dropindex", "dropcollection", "createcollection"]
                
                for keyword in unsafe_keywords:
                    if keyword in query_str:
                        self.logger.add_log(f"❌ Potentially unsafe operation detected: {keyword}. Aborting.")
                        return None
                
                self.logger.add_log("✅ Generated safe MongoDB query from natural language request")
                return query_dict
                
            except json.JSONDecodeError:
                self.logger.add_log("❌ Failed to parse MongoDB query as JSON")
                return None
        
        self.logger.add_log("❌ Failed to generate MongoDB query")
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
            
        data_stats = data.describe().to_string() if not data.empty else "No statistical data available."
            
        system_message = """You are an expert data analyst specializing in database analysis. 
        Provide insightful, actionable analysis based on the data. Format your response in well-structured markdown."""
        
        prompt = f"""
        You've been provided with data from a MongoDB query. 
        
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
            self.logger.add_log("✅ Generated data analysis")
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
        self.logger.add_log(f"🔍 Processing request: {request}")
        
        result = {
            "request": request,
            "success": False,
            "analysis": None,
            "visualization": None,
            "sample_data": None,
            "mongodb_query": None,
            "error": None
        }
        
        # Generate MongoDB query from request
        mongo_query = self.translate_to_mongodb_query(request)
        if not mongo_query:
            result["error"] = "Failed to translate your request to a MongoDB query. Please try rephrasing or provide more details."
            return result
        
        result["mongodb_query"] = mongo_query
        self.logger.add_log(f"🔍 Generated MongoDB query: {json.dumps(mongo_query, indent=2)}")
        
        # Execute the query
        data = self.db_manager.execute_query(
            collection=mongo_query.get("collection"),
            operation=mongo_query.get("operation", "find"),
            query=mongo_query.get("query", {}),
            projection=mongo_query.get("projection"),
            sort=mongo_query.get("sort"),
            limit=mongo_query.get("limit")
        )
        
        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            result["error"] = "No data found matching your request. Please try a different query."
            return result
            
        self.logger.add_log(f"✅ Retrieved data from MongoDB")
        
        # Convert to DataFrame if not already
        if not isinstance(data, pd.DataFrame):
            if isinstance(data, int):  # For count operations
                data = pd.DataFrame([{"count": data}])
            elif isinstance(data, list):  # For distinct operations or list results
                if all(isinstance(item, dict) for item in data):
                    data = pd.DataFrame(data)
                else:
                    data = pd.DataFrame({"values": data})
            else:
                result["error"] = "Unexpected data format returned from MongoDB."
                return result
        
        self.logger.add_log(f"✅ Processed {len(data)} rows of data")
        
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
    

"""
OpenAI API client wrapper for LLM functionality.
"""

import os
from typing import Optional, Dict, Any, List

import openai

class OpenAIClient:
    """Client for interacting with OpenAI API."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini"):
        """
        Initialize OpenAI client.
        
        Args:
            api_key: OpenAI API key
            model: Model to use for completions
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model
        self.client = None
        
    def initialize(self) -> bool:
        """
        Initialize OpenAI client with API key.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            if not self.api_key:
                print("❌ OpenAI API key not found. Set OPENAI_API_KEY environment variable or provide in config.")
                return False
                
            self.client = openai.OpenAI(api_key=self.api_key)
            print(f"✅ Connected to OpenAI API successfully (using model: {self.model})")
            return True
        except Exception as e:
            print(f"❌ OpenAI API connection error: {e}")
            return False
    
    def generate_completion(
        self, 
        system_message: str, 
        user_message: str,
        temperature: float = 0.2
    ) -> Optional[str]:
        """
        Generate a completion using OpenAI API.
        
        Args:
            system_message: System message to set context
            user_message: User message with prompt
            temperature: Temperature for generation (0-1)
            
        Returns:
            Generated text or None if failed
        """
        if not self.client:
            print("OpenAI client not initialized")
            return None
            
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                temperature=temperature
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"❌ Error generating completion: {e}")
            return None
            
    def get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Get embedding vector for text.
        
        Args:
            text: Text to embed
            
        Returns:
            List of embedding values or None if failed
        """
        if not self.client:
            print("OpenAI client not initialized")
            return None
            
        try:
            response = self.client.embeddings.create(
                input=text,
                model="text-embedding-ada-002"  # Use appropriate embedding model
            )
            
            return response.data[0].embedding
            
        except Exception as e:
            print(f"❌ Error generating embedding: {e}")
            return None
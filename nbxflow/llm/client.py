from typing import List, Dict, Any, Optional
import json

from ..config import settings
from ..utils.logging import get_logger

logger = get_logger(__name__)

class LLMClient:
    """Client for interacting with various LLM providers."""
    
    def __init__(self, provider: Optional[str] = None, model: Optional[str] = None, 
                 api_key: Optional[str] = None, endpoint: Optional[str] = None):
        self.provider = provider or settings.llm_provider
        self.model = model or settings.llm_model
        self.api_key = api_key or settings.llm_api_key
        self.endpoint = endpoint or settings.llm_endpoint
        self._client = None
        
    def _init_client(self):
        """Initialize the appropriate LLM client."""
        if self._client is not None:
            return
            
        try:
            if self.provider == "openai":
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key)
                logger.info("Initialized OpenAI client")
                
            elif self.provider == "azure-openai":
                from openai import AzureOpenAI
                if not self.endpoint:
                    raise ValueError("Azure OpenAI endpoint is required")
                self._client = AzureOpenAI(
                    azure_endpoint=self.endpoint,
                    api_key=self.api_key,
                    api_version="2024-02-01"
                )
                logger.info(f"Initialized Azure OpenAI client for {self.endpoint}")
                
            else:
                raise ValueError(f"Unsupported LLM provider: {self.provider}")
                
        except ImportError as e:
            logger.error(f"LLM client dependencies not installed: {e}")
            raise RuntimeError("Install LLM dependencies with: pip install 'nbxflow[llm]'")
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            raise
    
    def chat(self, messages: List[Dict[str, str]], temperature: float = 0.0, 
             max_tokens: Optional[int] = None, response_format: Optional[Dict] = None) -> str:
        """
        Send a chat completion request.
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            temperature: Sampling temperature (0.0 to 2.0)
            max_tokens: Maximum tokens in response
            response_format: Optional response format specification
            
        Returns:
            The response content as a string
        """
        if not self.api_key:
            raise ValueError(f"API key not configured for provider: {self.provider}")
        
        self._init_client()
        
        try:
            # Prepare request parameters
            request_params = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature
            }
            
            if max_tokens:
                request_params["max_tokens"] = max_tokens
            
            # Handle response format for structured outputs
            if response_format and response_format.get("type") == "json_object":
                request_params["response_format"] = {"type": "json_object"}
            
            # Make request
            response = self._client.chat.completions.create(**request_params)
            content = response.choices[0].message.content
            
            logger.debug(f"LLM request completed. Tokens used: {response.usage.total_tokens if response.usage else 'unknown'}")
            
            return content
            
        except Exception as e:
            logger.error(f"LLM request failed: {e}")
            raise RuntimeError(f"LLM request failed: {str(e)}")
    
    def is_available(self) -> bool:
        """Check if the LLM client is properly configured and available."""
        try:
            if not self.api_key:
                return False
            self._init_client()
            return True
        except Exception:
            return False

# Global client instance
_default_client: Optional[LLMClient] = None

def get_llm_client() -> LLMClient:
    """Get the default LLM client."""
    global _default_client
    if _default_client is None:
        _default_client = LLMClient()
    return _default_client

def chat(messages: List[Dict[str, str]], **kwargs) -> str:
    """Convenience function for chat completion."""
    client = get_llm_client()
    return client.chat(messages, **kwargs)

def is_llm_available() -> bool:
    """Check if LLM functionality is available."""
    try:
        client = get_llm_client()
        return client.is_available()
    except Exception:
        return False
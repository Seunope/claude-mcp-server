import os
import datetime

class Logger:
    """Class for handling logging operations to a file."""
    
    def __init__(self, log_file_path: str = None):
        """
        Initialize the logger with a specific log file path.
        
        Args:
            log_file_path (str, optional): Path to the logs file. If None, uses default path.
        """
        if log_file_path is None:
            self.log_file = os.path.join(os.path.dirname(__file__), "logs.txt")
        else:
            self.log_file = log_file_path
        self._ensure_file()
    
    def _ensure_file(self) -> None:
        """Ensure the log file exists."""
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w") as f:
                f.write("")
    
    def add_log(self, message: str) -> str:
        """
        Append a new log to the file.
        
        Args:
            message (str): The log content to be added.
            
        Returns:
            str: Confirmation message indicating the log was saved.
        """
        timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        log_entry = f"{timestamp} {message}"
        
        with open(self.log_file, "a") as f:
            f.write(log_entry + "\n")
        return "Log saved!"
    
    def get_logs(self) -> str:
        """
        Read and return all logs from the log file.
        
        Returns:
            str: All logs as a single string separated by line breaks.
                 If no logs exist, a default message is returned.
        """
        self._ensure_file()
        with open(self.log_file, "r") as f:
            content = f.read().strip()
        return content or "No logs yet."
    
    def get_latest_log(self) -> str:
        """
        Get the most recently added log from the log file.
        
        Returns:
            str: The last log entry. If no logs exist, a default message is returned.
        """
        self._ensure_file()
        with open(self.log_file, "r") as f:
            lines = f.readlines()
        return lines[-1].strip() if lines else "No logs yet."


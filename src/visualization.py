"""
Data visualization functions for generating charts from query results.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import base64
from io import BytesIO
from typing import Optional, List, Dict, Any, Tuple

def detect_chart_type(data: pd.DataFrame, request: str) -> Tuple[str, str, str]:
    """
    Detect the most appropriate chart type based on data and request.
    
    Args:
        data: DataFrame with query results
        request: Original natural language request
        
    Returns:
        Tuple of (chart_type, x_column, y_column)
    """
    # Initial setup
    chart_type = "bar"  # Default chart type
    
    # Extract column types
    numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = data.select_dtypes(exclude=['number']).columns.tolist()
    date_cols = [col for col in data.columns if 'date' in col.lower() or 'time' in col.lower()]
    
    # Keywords that suggest chart types
    pie_keywords = ['distribution', 'proportion', 'breakdown', 'percentage', 'pie']
    line_keywords = ['trend', 'over time', 'timeline', 'change', 'growth']
    scatter_keywords = ['correlation', 'relationship', 'scatter', 'versus', 'vs']
    bar_keywords = ['compare', 'comparison', 'rank', 'top', 'bar']
    
    # Determine chart type from request
    if any(keyword in request.lower() for keyword in pie_keywords):
        chart_type = "pie"
    elif any(keyword in request.lower() for keyword in line_keywords):
        chart_type = "line"
    elif any(keyword in request.lower() for keyword in scatter_keywords):
        chart_type = "scatter"
    elif any(keyword in request.lower() for keyword in bar_keywords):
        chart_type = "bar"
    
    # Select appropriate columns based on chart type
    x_col = None
    y_col = None
    
    if chart_type == "pie":
        # For pie charts, we need one categorical and one numeric column
        if categorical_cols and numeric_cols:
            x_col = categorical_cols[0]
            y_col = numeric_cols[0]
        elif len(numeric_cols) >= 2:
            # If no categorical, convert one numeric to categorical
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
    
    elif chart_type == "line":
        # For line charts, prefer date columns for x-axis
        if date_cols and numeric_cols:
            x_col = date_cols[0]
            y_col = numeric_cols[0]
        elif len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
        elif categorical_cols and numeric_cols:
            x_col = categorical_cols[0]
            y_col = numeric_cols[0]
    
    elif chart_type == "scatter":
        # For scatter plots, we need two numeric columns
        if len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
    
    elif chart_type == "bar":
        # For bar charts, prefer categorical for x-axis
        if categorical_cols and numeric_cols:
            x_col = categorical_cols[0]
            y_col = numeric_cols[0]
        elif len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
    
    # If we still couldn't determine columns, use defaults
    if not x_col or not y_col:
        if len(data.columns) >= 2:
            x_col = data.columns[0]
            if x_col in numeric_cols and len(data.columns) > 2:
                y_col = data.columns[2]
            else:
                y_col = data.columns[1]
        else:
            # If only one column, use index as x and column as y
            x_col = "index"
            y_col = data.columns[0]
    
    return chart_type, x_col, y_col

def create_visualization(data: pd.DataFrame, request: str) -> Optional[str]:
    """
    Create a visualization based on the data and request.
    
    Args:
        data: DataFrame with query results
        request: Original natural language request
        
    Returns:
        Base64 encoded string of the visualization or None if failed
    """
    if data is None or data.empty or len(data.columns) < 1:
        return None
    
    try:
        # Create a copy to avoid modifying the original
        df = data.copy()
        
        # Reset index if needed for plotting
        if df.index.name:
            df = df.reset_index()
        
        # Determine chart type and columns
        chart_type, x_col, y_col = detect_chart_type(df, request)
        
        # If x_col is "index", use the DataFrame index
        if x_col == "index":
            df = df.reset_index()
            x_col = "index"
        
        # Create figure
        plt.figure(figsize=(10, 6))
        
        # Create appropriate chart
        if chart_type == "bar":
            # Limit to top 20 items for readability
            if len(df) > 20:
                top_df = df.nlargest(20, y_col) if y_col in df.columns else df.head(20)
                sns.barplot(x=x_col, y=y_col, data=top_df)
                plt.title(f"Top 20 by {y_col}")
            else:
                sns.barplot(x=x_col, y=y_col, data=df)
                plt.title(f"Bar Chart: {y_col} by {x_col}")
            
            # Rotate x labels if there are many categories
            if len(df[x_col].unique()) > 5:
                plt.xticks(rotation=45, ha='right')
        
        elif chart_type == "line":
            sns.lineplot(x=x_col, y=y_col, data=df)
            plt.title(f"Line Chart: {y_col} over {x_col}")
            
            # Rotate x labels if there are many points
            if len(df[x_col].unique()) > 5:
                plt.xticks(rotation=45, ha='right')
        
        elif chart_type == "pie":
            # For pie charts, we need to aggregate data if there are too many categories
            if len(df[x_col].unique()) > 10:
                # Get top 9 and group others
                top_values = df.nlargest(9, y_col)[x_col].unique()
                mask = df[x_col].isin(top_values)
                pie_data = pd.concat([
                    df[mask].groupby(x_col)[y_col].sum(),
                    pd.Series({
                        'Others': df[~mask][y_col].sum()
                    })
                ])
            else:
                pie_data = df.groupby(x_col)[y_col].sum()
            
            plt.pie(pie_data, labels=pie_data.index, autopct='%1.1f%%')
            plt.axis('equal')
            plt.title(f"Distribution of {y_col} by {x_col}")
        
        elif chart_type == "scatter":
            sns.scatterplot(x=x_col, y=y_col, data=df)
            plt.title(f"Scatter Plot: {y_col} vs {x_col}")
        
        plt.tight_layout()
        
        # Save to BytesIO object
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Convert to base64 for embedding
        img_str = base64.b64encode(buffer.read()).decode('utf-8')
        plt.close()
        
        return img_str
        
    except Exception as e:
        print(f"❌ Error creating visualization: {e}")
        return None
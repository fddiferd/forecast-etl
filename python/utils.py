import re
import snowflake.connector
import pandas as pd
import os
from pathlib import Path

class SnowflakeContext:
    """A context manager for a Snowflake connection. This manages connections, queries,
    and cleansing of responses to standardized format.
    """

    def __enter__(self):
        self.ctx = self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.ctx.close()

    def connect(self) -> snowflake.connector.connection.SnowflakeConnection:
        """
        Connect to Snowflake.

        This function connects to Snowflake using environment
        variables.

        Returns:
        snowflake.connector.connection.SnowflakeConnection: The Snowflake connection.
        """
        ctx = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            account='sw09563.us-east-1',
            authenticator='externalbrowser',
            warehouse=os.environ.get('SNOWFLAKE_WH', 'SPLIT_TESTING_L'),
        )
        return ctx

    def fetch(self, query: str) -> pd.DataFrame:
        """fetch the data, and return cleaned results

        Args:
            query (str): the query to execute

        Returns:
            pd.DataFrame: the cleaned results
        """
        cursor = self.ctx.cursor()
        try:
            cursor.execute(query)
            return self.convert_snowflake_response(cursor.fetch_pandas_all())
        finally:
            cursor.close()

    def convert_snowflake_response(self, response: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a Snowflake response DataFrame by removing metadata columns and
        converting column names to lowercase.

        Parameters:
        response (pd.DataFrame): The Snowflake response DataFrame.

        Returns:
        pd.DataFrame: The cleaned DataFrame.
        """
        # Delete all metadata columns that start with an underscore
        response = response.loc[:, ~response.columns.str.startswith('_')]
        # Convert all column names to lowercase

        response.columns = [x.lower() for x in response.columns]  # type: ignore

        return response

    def format_variables(self, query: str) -> str:
        """
        Replace the SQL variable format $var with the python format '{var}'.

        Parameters:
        query (str): The SQL query.

        Returns:
        str: The formatted SQL query.
        """
        return re.sub(r'\$(\w+)', r"'{\1}'", query)

def get_data(query_input: str, variables: dict = {}, cache: bool = True) -> pd.DataFrame:
    """Get data from Snowflake

    Args:
        query_input (str): the filename of the SQL file or a SQL query string
        variables (dict, optional): A dict of the key-value pairs used for formatting the query. Defaults to {}.
        cache (bool, optional): Whether or not to use the cache. If False, will ignore any generated
        files, and overwrite them. Defaults to True.

    Raises:
        FileNotFoundError: Raised if the file does not exist

    Returns:
        pd.DataFrame: The query result
    """
    # Define cache directory
    cache_dir = Path('../sql/caches')
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Prepend 'SQL/' to query_input if it is a file
    # if not query_input.startswith('sql/'):
    #     query_input = 'sql/' + query_input

    # Determine if the input is a file or a query string
    is_file = query_input.lower().endswith('.sql')
    is_query = query_input.strip().lower().startswith('select')

    if is_file:
        # See if the file exists, otherwise raise an error
        query_file = Path(query_input)
        if not query_file.exists():
            raise FileNotFoundError(f"File {query_input} not found")
        
        # Read the query from the file
        query = query_file.read_text()
        # Set cache filename based on the input filename
        cache_file = cache_dir / query_file.with_suffix('.parquet').name
    elif is_query:
        # Use the input directly as the query
        query = query_input
        # Set a unique cache filename based on the query hash
        cache_file = cache_dir / f"{hash(query)}.parquet"
    else:
        raise ValueError("The input must be a filename ending with '.sql' or a SQL query starting with 'select'")

    # If cache is enabled and the cache file exists, return its contents
    if cache and cache_file.exists():
        return pd.read_parquet(cache_file)

    # Otherwise, format the query with the provided variables
    with SnowflakeContext() as context:
        # If the query has SQL style variables, convert them to Python style format strings
        # query = context.format_variables(query)
        # Format the query with the variables
        # query = query.format(**variables)
        # Fetch the result
        result = context.fetch(query)
        # Save the result to the cache file
        result.to_parquet(cache_file, index=False)
        # Return the results
        return result
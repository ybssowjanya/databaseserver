from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from openai import AzureOpenAI
import re
from pydantic import BaseModel
import time
from azure.storage.blob import BlobServiceClient
import json
from datetime import datetime

app = FastAPI()

def log_message(message: str):
    """Helper function to print log messages with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

class DatabaseConn:
    def __init__(self, agent_id: str):
        log_message(f"Initializing DatabaseConn for agent_id: {agent_id}")
        self.agent_id = agent_id
        self.connection_string = (
            "DefaultEndpointsProtocol=https;AccountName=chatdocstorage;"
            "AccountKey=nmW8RvkMGU4O63dppdRQzzH08wNBS0gswaTdwo+Zx/SUVC4YVwnW03U3fVu13+qrEG7/hAJst4cE+AStduBNVA==;"
            "EndpointSuffix=core.windows.net"
        )
        self.container_name = "databaseconnections"
        log_message(f"Container name set to: {self.container_name}")
        self.sqlalchemy_url = self._get_db_connection_string()

    def _get_db_connection_string(self):
        log_message(f"Fetching database connection string for agent_id: {self.agent_id}")
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            log_message("BlobServiceClient initialized successfully")
            container_client = blob_service_client.get_container_client(self.container_name)
            blob_name = f"{self.agent_id}_database.json"
            log_message(f"Attempting to access blob: {blob_name}")
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            log_message(f"Blob data retrieved for {blob_name}")
            db_config = json.loads(blob_data.decode('utf-8'))
            log_message(f"Database config loaded: {json.dumps(db_config, indent=2)}")
            required_fields = ['username', 'password', 'server', 'databasename']
            if not all(field in db_config for field in required_fields):
                missing_fields = [field for field in required_fields if field not in db_config]
                log_message(f"Missing required fields in db_config: {missing_fields}")
                raise ValueError(f"Missing required database configuration fields: {missing_fields}")
            sqlalchemy_url = (
                f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                f"@{db_config['server']}:1433/{db_config['databasename']}"
                "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
            )
            log_message(f"SQLAlchemy URL constructed successfully")
            return sqlalchemy_url
        except Exception as e:
            log_message(f"Error in _get_db_connection_string: {str(e)}")
            raise HTTPException(status_code=400, detail=f"❌ Failed to load database configuration: {str(e)}")

    def connect_to_db(self):
        log_message("Attempting to connect to database")
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                log_message(f"Connection attempt {retries + 1}/{max_retries}")
                engine = create_engine(self.sqlalchemy_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    log_message("Database connection test successful")
                return engine
            except SQLAlchemyError as e:
                retries += 1
                log_message(f"Connection attempt {retries} failed: {str(e)}")
                if retries == max_retries:
                    log_message(f"Database connection failed after {max_retries} attempts")
                    raise HTTPException(status_code=400, detail=f"❌ Database connection failed after {max_retries} attempts: {str(e)}")
                time.sleep(1)

    def question_db(self, query):
        log_message(f"Executing query: {query}")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = [dict(row._mapping) for row in result]
                log_message(f"Query executed successfully, retrieved {len(rows)} rows")
                return rows
        except SQLAlchemyError as e:
            log_message(f"Query execution failed: {str(e)}")
            raise HTTPException(status_code=400, detail=f"❌ Query failed: {str(e)}")

    def get_db_schema(self):
        log_message("Fetching database schema")
        self.engine = self.connect_to_db()
        try:
            inspector = inspect(self.engine)
            schema = {}
            for table_name in inspector.get_table_names():
                columns = inspector.get_columns(table_name)
                schema[table_name] = [
                    {"column_name": col["name"], "data_type": str(col["type"])} for col in columns
                ]
            log_message(f"Schema retrieved: {json.dumps(schema, indent=2)}")
            return schema
        except SQLAlchemyError as e:
            log_message(f"Schema extraction error: {str(e)}")
            return {}

    def extract_sql_query(self, response_content):
        log_message(f"Extracting SQL query from response: {response_content}")
        match = re.search(r'```(?:sql)?\s*(.*?)```', response_content, re.DOTALL)
        if match:
            sql_query = match.group(1).strip()
            log_message(f"Extracted SQL query: {sql_query}")
            return sql_query
        log_message("No SQL query found in response")
        return None

    def generate_sql_query(self, query: str, schema: dict):
        log_message(f"Generating SQL query for question: {query}")
        log_message(f"Using schema: {json.dumps(schema, indent=2)}")
        openai_client = AzureOpenAI(
            api_key="El3PVLZmsFF8VVQB3KaKyouiKT4GFYmL4ZdxbaodTzR2lOx2PZ0eJQQJ99BDACYeBjFXJ3w3AAABACOG5JKH",
            api_version="2024-02-15-preview",
            azure_endpoint="https://chatwithdoc-ai.openai.azure.com/",
            azure_deployment="gpt-4o"
        )
        system_prompt = f"""
        You are a smart assistant that converts natural language question into a SQL query.
        Check for table names in the database and only return the SQL query to extract the data that is useful for the query.
        Only respond with the SQL Query.
        Do not include any explanation.
        Database Schema:
        {schema}
        """
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                temperature=0.8,
                max_tokens=500
            )
            response_content = response.choices[0].message.content
            log_message(f"OpenAI response: {response_content}")
            sql_query = self.extract_sql_query(response_content)
            if sql_query:
                log_message(f"Generated SQL query: {sql_query}")
                return sql_query
            else:
                log_message("Failed to extract SQL query from OpenAI response")
                raise ValueError("No valid SQL query generated")
        except Exception as e:
            log_message(f"Error generating SQL query: {str(e)}")
            raise HTTPException(status_code=400, detail=f"❌ Failed to generate SQL query: {str(e)}")

class QuestionRequest(BaseModel):
    question: str

@app.post("/questiondb/{agent_id}")
def question_to_db(agent_id: str, request: QuestionRequest):
    log_message(f"Processing question for agent_id: {agent_id}, question: {request.question}")
    try:
        handler = DatabaseConn(agent_id)
        schema = handler.get_db_schema()
        sql_query = handler.generate_sql_query(request.question, schema)
        results = handler.question_db(sql_query)
        log_message(f"Question processed successfully, results: {json.dumps(results, indent=2)}")
        return {"Final Results": results}
    except Exception as e:
        log_message(f"Error processing question: {str(e)}")
        raise HTTPException(status_code=400, detail=f"❌ Failed to process question: {str(e)}")

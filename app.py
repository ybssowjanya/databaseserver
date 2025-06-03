from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from openai import AzureOpenAI
import re
from pydantic import BaseModel
import time
from azure.storage.blob import BlobServiceClient
import json

app = FastAPI()

class DatabaseConn:
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.connection_string = (
            "DefaultEndpointsProtocol=https;AccountName=chatdocstorage;"
            "AccountKey=nmW8RvkMGU4O63dppdRQzzH08wNBS0gswaTdwo+Zx/SUVC4YVwnW03U3fVu13+qrEG7/hAJst4cE+AStduBNVA==;"
            "EndpointSuffix=core.windows.net"
        )
        self.container_name = "databaseconnections"
        self.sqlalchemy_url = self._get_db_connection_string()

    def _get_db_connection_string(self):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            container_client = blob_service_client.get_container_client(self.container_name)
            blob_name = f"{self.agent_id}_database.json"
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            db_config = json.loads(blob_data.decode('utf-8'))
            required_fields = ['username', 'password', 'server', 'databasename']
            if not all(field in db_config for field in required_fields):
                raise ValueError("Missing required database configuration fields")
            return (
                f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                f"@{db_config['server']}:1433/{db_config['databasename']}"
                "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"❌ Failed to load database configuration: {str(e)}")

    def connect_to_db(self):
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                engine = create_engine(self.sqlalchemy_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return engine
            except SQLAlchemyError as e:
                retries += 1
                if retries == max_retries:
                    raise HTTPException(status_code=400, detail=f"❌ Database connection failed after {max_retries} attempts: {str(e)}")
                time.sleep(1)

    def question_db(self, query):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = [dict(row._mapping) for row in result]
                return rows
        except SQLAlchemyError as e:
            raise HTTPException(status_code=400, detail=f"❌ Query failed: {str(e)}")

    def get_db_schema(self):
        self.engine = self.connect_to_db()
        try:
            inspector = inspect(self.engine)
            schema = {}
            for table_name in inspector.get_table_names():
                columns = inspector.get_columns(table_name)
                schema[table_name] = [
                    {"column_name": col["name"], "data_type": str(col["type"])} for col in columns
                ]
            return schema
        except SQLAlchemyError as e:
            print(f"❌ Schema extraction error: {str(e)}")
            return {}

    def extract_sql_query(self, response_content):
        match = re.search(r'```(?:sql)?\s*(.*?)```', response_content, re.DOTALL)
        return match.group(1).strip() if match else None

    def generate_sql_query(self, query: str, schema: str):
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
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            temperature=0.8,
            max_tokens=500
        )
        return self.extract_sql_query(response.choices[0].message.content)

class QuestionRequest(BaseModel):
    question: str

@app.post("/questiondb/{agent_id}")
def question_to_db(agent_id: str, request: QuestionRequest):
    handler = DatabaseConn(agent_id)
    schema = handler.get_db_schema()
    sql_query = handler.generate_sql_query(request.question, schema)
    results = handler.question_db(sql_query)
    return {"Final Results": results}

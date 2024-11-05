import azure.functions as func
import os
import json
import requests
import psycopg2
from langchain_community.utilities import SQLDatabase

# Configurações de ambiente (variáveis)
azure_api_key = os.environ.get("AZURE_OPENAI_API_KEY")
endpoint_url = os.environ.get("AZURE_OPENAI_ENDPOINT")

# Configurações do PostgreSQL
postgres_user = os.environ.get("POSTGRES_USER")
postgres_password = os.environ.get("POSTGRES_PASSWORD")
postgres_host = os.environ.get("POSTGRES_HOST")
postgres_dbname = os.environ.get("POSTGRES_DBNAME")
postgres_port = os.environ.get("POSTGRES_PORT")

conn_str = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_dbname}"
db = SQLDatabase.from_uri(conn_str)

def get_database_schema():
    conn = psycopg2.connect(
        dbname=postgres_dbname,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )
    cursor = conn.cursor()
    schema = {}

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns = cursor.fetchall()
        schema[table_name] = [column[0] for column in columns]  # Nome das colunas

    conn.close()
    return schema

def natural_language_to_sql(natural_language_query):
    schema = get_database_schema()
    schema_description = "\n".join([f"Tabela {table}: {columns}" for table, columns in schema.items()])
    
    prompt = (
        f"Com base na estrutura do banco de dados abaixo, responda à pergunta '{natural_language_query}' "
        f"usando uma consulta SQL apropriada.\n\nEstrutura do banco de dados:\n{schema_description}\n\n"
        "Apenas a consulta SQL:"
    )

    headers = {
        "Content-Type": "application/json",
        "api-key": azure_api_key
    }
    data = {
        "messages": [
            {"role": "system", "content": "Você é um assistente SQL."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0,
        "max_tokens": 800,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0
    }

    response = requests.post(endpoint_url, headers=headers, json=data)
    response.raise_for_status()

    response_data = response.json()
    sql_query = response_data['choices'][0]['message']['content'].strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
    return sql_query

def execute_query(query):
    try:
        conn = psycopg2.connect(
            dbname=postgres_dbname,
            user=postgres_user,
            password=postgres_password,
            host=postgres_host,
            port=postgres_port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results
    except Exception as e:
        print("Erro ao executar a consulta:", e)
        return None

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Obter a consulta do usuário da requisição
        natural_language_query = req.params.get('query')
        if not natural_language_query:
            return func.HttpResponse(
                "Por favor, forneça uma consulta SQL em linguagem natural como parâmetro 'query'.",
                status_code=400
            )

        # Converter linguagem natural para SQL
        sql_query = natural_language_to_sql(natural_language_query)
        print("Consulta SQL Gerada:", sql_query)

        # Executar a consulta SQL
        response = execute_query(sql_query)
        if response is not None:
            return func.HttpResponse(
                json.dumps({"response": response}),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                "Nenhuma resposta ou ocorreu um erro.",
                status_code=500
            )
    except Exception as e:
        return func.HttpResponse(
            str(e),
            status_code=500
        )

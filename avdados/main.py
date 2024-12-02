import os
import psycopg2
import requests
import azure.functions as func
import json
from datetime import date, datetime
from decimal import Decimal

# Configurações do ambiente
azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
endpoint_url = os.getenv("AZURE_OPENAI_ENDPOINT")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_dbname = os.getenv("POSTGRES_DBNAME")
postgres_port = os.getenv("POSTGRES_PORT", 5432)

# Serialização JSON para tipos não nativos
def json_serial(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Tipo {type(obj)} não é serializável em JSON")

# Função principal da Azure Function
async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Obtendo a consulta natural
        natural_language_query = req.params.get('query')
        if not natural_language_query:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                natural_language_query = req_body.get('query')

        if not natural_language_query:
            return func.HttpResponse(
                "Por favor, forneça uma consulta em linguagem natural no parâmetro 'query'.",
                status_code=400
            )

        # Gera a consulta SQL com base na entrada natural
        sql_query = generate_sql_from_natural_language(natural_language_query)
        print("Consulta SQL Gerada:", sql_query)

        # Executa a consulta no banco de dados
        response = execute_sql_query(sql_query)

        # Retorna a resposta como JSON
        return func.HttpResponse(
            json.dumps(response, default=json_serial),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Erro: {str(e)}", status_code=500)

# Gera SQL com base na entrada natural usando Azure OpenAI
def generate_sql_from_natural_language(query):
    schema_description = get_database_schema_description()
    prompt = (
        f"Com base na estrutura do banco de dados abaixo, responda à pergunta '{query}' "
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

    # Remover delimitadores ```sql e ```
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]  # Remove "```sql"
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]  # Remove "```"

    return sql_query.strip()


# Obtém a descrição do esquema do banco de dados
def get_database_schema_description():
    conn = psycopg2.connect(
        dbname=postgres_dbname,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port
    )
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()
    schema = {}
    for table_name in tables:
        table_name = table_name[0]
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns = cursor.fetchall()
        schema[table_name] = [column[0] for column in columns]

    conn.close()

    return "\n".join([f"Tabela {table}: {columns}" for table, columns in schema.items()])

# Executa a consulta SQL no banco de dados
def execute_sql_query(query):
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
    conn.close()

    return results

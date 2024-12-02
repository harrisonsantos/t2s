import os
import aiohttp
from dotenv import load_dotenv
from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.schema import ActivityTypes, Attachment, Activity

load_dotenv()

class MyBot(ActivityHandler):
    def __init__(self):
        super().__init__()
        self.functions_base_url = os.getenv("FUNCTIONS_BASE_URL", "http://localhost:7071")

    async def on_message_activity(self, turn_context: TurnContext):
        user_message = turn_context.activity.text
        try:
            # Chama a função Azure para obter a resposta
            response_data = await self.call_azure_function(user_message)
            formatted_response = self.format_response(response_data)

            # Envia a resposta formatada ao usuário
            await turn_context.send_activity(formatted_response)

        except Exception as e:
            print("Erro ao processar a mensagem:", e)
            await turn_context.send_activity("Desculpe, ocorreu um erro ao processar sua solicitação.")

    async def call_azure_function(self, query: str):
        async with aiohttp.ClientSession() as session:
            url = f"{self.functions_base_url}/api/avdados"
            headers = {"Content-Type": "application/json"}
            payload = {"query": query}

            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_details = await response.text()
                    raise ValueError(f"Erro ao chamar a função Azure: {response.status} - {error_details}")

    def format_response(self, response_data):
        # Formata a resposta recebida da Azure Function
        if isinstance(response_data, list):
            return "\n".join([str(item) for item in response_data])
        elif isinstance(response_data, dict):
            return str(response_data)
        else:
            return response_data

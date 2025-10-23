"""
    Módulo responsável por atualizar o arquivo Excel no SharePoint.
    O arquivo é gerado a partir de um JSON contendo os dados extraídos via webscraping.
    Utiliza a API do SharePoint para realizar o upload do arquivo.
"""

import io
from datetime import datetime
from urllib.parse import quote
import pandas
import requests
from requests.exceptions import RequestException

from src.common import MAX_RETRIES, TIMEOUT, login, handle_request_exception
from src.setup import Config, Logger, get_env_values

nome_site = Config.get("INIT", "SITE_NAME")
nome_dominio = Config.get("INIT", "DOMAIN_NAME")

if nome_site == "none":
    Logger.critical("Nome de site não existe! Ajuste isso no arquivo settings.ini.")
    raise ValueError("Nome do site não existe! Ajuste isso no arquivo settings.ini. (none)")
if nome_dominio == "none":
    Logger.critical("Nome do domínio não existe! Ajuste isso no arquivo settings.ini.")
    raise ValueError("Nome do domínio não existe! Ajuste isso no arquivo settings.ini. (none)")

# criar pasta e arquivo no sharepoint
# caso modificar o caminho, mudar as variáveis abaixo
FILE_PATH = (
    f"/sites/{nome_site}/Shared Documents/"
    "Configurações - Monitoramento BIs/refresh-schedule-pbis-log.xlsx"
)
FILE_URL = (
    f"https://{nome_dominio}.sharepoint.com/sites/{nome_site}/_api/web/"
    f"GetFileByServerRelativeUrl('{quote((FILE_PATH), safe='/')}')/$value"
)

SCOPE = f"https://{nome_dominio}.sharepoint.com/.default" # escopo de permissividade

class UpdateSharepointFile:
    """
        Classe responsável por atualizar o arquivo Excel no SharePoint.
        O arquivo é gerado a partir de um JSON contendo os dados extraídos via webscraping.

        Métodos:
        - get_data(): Retorna o dataframe com os dados que serão enviados para o SharePoint.
        - put_in_sharepoint(json): Publica o arquivo Excel atualizado no SharePoint.
    """

    def __init__(self) -> None:
        self._access_token = get_env_values().get('ACCESS_TOKEN_SHAREPOINT')

        self._data = {}

    def get_data(self) -> dict:
        """
            Retorna o dataframe com os dados que serão enviados para o SharePoint.
            Útil para verificar os dados antes do envio.
        """

        return self._data

    def put_in_sharepoint(self, data_json: dict) -> None:
        """
            Envia o arquivo Excel com os dados para o SharePoint.
            O arquivo é gerado a partir do JSON fornecido.

            Parâmetros:
            - json (dict): Dicionário contendo os dados a serem enviados.    
        """

        Logger.info("[REQUESTS] Formatando arquivo...")
        rows = []

        for workspace, objects in data_json.items():
            if isinstance(objects, list):
                for dataset in objects:
                    rows.append({"canceled": dataset})
            else:
                for _, dataset in objects.items():
                    for hour in dataset.get("times", []):
                        rows.append({
                            "lastupdated": datetime.now(),
                            "name": dataset.get("name", "-"),
                            "workspace": workspace,
                            "time": hour,
                            "enabled": dataset.get("enabled", "-")
                        })

        self._data = pandas.DataFrame(rows).fillna("-")

        excel_buffer = io.BytesIO()
        self._data.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        Logger.info("[REQUESTS] Publicando arquivo no Sharepoint...")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                request = requests.put(
                    url=FILE_URL,
                    headers={
                        "Authorization": f"Bearer {self._access_token}",
                    },
                    data=excel_buffer.getvalue(),
                    timeout=TIMEOUT
                )

                if request.status_code not in (200, 201, 204):
                    raise RequestException(
                        f"Status: {request.status_code}"
                        f" | Resposta: {request.text}"
                    )
                Logger.info("[REQUESTS] Arquivo atualizado com sucesso no SharePoint.")
                return
            except RequestException as sharepoint_error:
                handle_request_exception(
                    error=sharepoint_error,
                    attempt=attempt,
                    get_new_token=lambda: setattr(self, "_access_token", login(SCOPE))
                )

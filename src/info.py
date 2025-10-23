"""
    Módulo responsável por coletar as informações do Power BI Online via webscrapping.
    Os principais dados coletados são: data hora de última atualização, atualizado hoje,
    erro na última atualização e data hora da próxima atualização.
"""

import sys
import time
import json
from urllib.parse import quote
import requests
from requests.exceptions import RequestException

from src.common import RETRY_DELAY, MAX_RETRIES, TIMEOUT
from src.common import login, handle_request_exception
from src.setup import Logger, get_env_values

BASE_URL = "https://api.powerbi.com/v1.0/myorg/groups"
LOGIN_WORDS = ("singleSignOn", "signin", "login")

REQUEST_INTERVAL = 2

SCOPE = "https://analysis.windows.net/powerbi/api/.default" # escopo de permissividade
class WebExtractor:
    """
        Classe responsável por coletar os dados do Power BI Online.
        As informações são coletadas workspace por workspace.

        Métodos:
        - get_info(): Método principal que executa a coleta dos dados.
    """

    def __init__(self) -> None:
        self._access_token = get_env_values().get('ACCESS_TOKEN')

        self._workspaces = {}
        self._objects = {}
        self._data = {}
        self._canceled = {}

    def _get_workspaces(self) -> str:
        """
            Função responsável por pegar os workspaces do diretório.
            Retorna os workspaces no formato de lista.
        """

        for attempt in range(1, MAX_RETRIES + 1, 1):
            try:
                headers = {
                    "Authorization": f"Bearer {self._access_token}" 
                }

                response = requests.get(url=BASE_URL, headers=headers, timeout=TIMEOUT)
                response.raise_for_status()

                Logger.info("[REQUESTS] Token válido!")

                response = response.json()

                all_workspaces = []

                Logger.info("[REQUESTS] Pegando as workspaces...")
                for workspace in response.get("value", []):
                    if (name := workspace.get("name")) and (wid := workspace.get("id")):
                        all_workspaces.append({ "name": name, "id": wid })

                return json.dumps(
                    list({(w["name"], w["id"]): w for w in all_workspaces}.values()),
                    indent=4
                )
            except RequestException as error:
                self._access_token = handle_request_exception(
                    attempt=attempt,
                    error=error,
                    scope=SCOPE
                )
        return ""

    def _get_w_objects(self) -> str:
        """
            Propriedade que retorna todos os modelos semânticos e fluxos.
        """

        for attempt in range(1, MAX_RETRIES + 1, 1):
            try:
                all_content = {}

                headers = {
                    "Authorization": f"Bearer {self._access_token}" 
                }

                Logger.info("[REQUESTS] Pegando o conteúdo das workspaces...")

                for workspace in json.loads(self.workspaces):
                    all_content.setdefault(workspace['name'], {
                            "workspace_id": workspace['id'],
                            "dataflows": {},
                            "datasets": {}
                        }
                    )

                    dataflow_url = BASE_URL + f"/{workspace['id']}/dataflows"
                    response = requests.get(url=dataflow_url, headers=headers, timeout=TIMEOUT)

                    if response.status_code == 403:
                        Logger.warning(
                            "[REQUESTS] Dataflows - "
                            "Acesso negado à workspace %s. Pulando...", workspace['name']
                        )
                        continue

                    response.raise_for_status()
                    dataflows = response.json()

                    for dataflow in dataflows.get("value"):
                        all_content[workspace['name']]['dataflows'].update(
                            {
                                dataflow.get("name", "none"): dataflow.get("objectId", "none")
                            }
                        )

                    dataset_url = BASE_URL + f"/{workspace['id']}/datasets"
                    response = requests.get(url=dataset_url, headers=headers, timeout=TIMEOUT)

                    if response.status_code == 403:
                        Logger.warning(
                            "[REQUESTS] Datasets - "
                            "Acesso negado à workspace %s. Pulando...", workspace['name']
                        )
                        continue

                    response.raise_for_status()
                    datasets = response.json()

                    for dataset in datasets.get("value"):
                        all_content[workspace['name']]['datasets'].update(
                            {
                                dataset.get("name", "none"): dataset.get("id", "none")
                            }
                        )
                return json.dumps(
                    all_content,
                    indent=4
                )
            except RequestException as error:
                Logger.error("[REQUESTS] Tentativa %s. Erro: %s", attempt, error)
                if attempt < MAX_RETRIES:
                    Logger.info("[REQUESTS] Tentando novamente em %s segundos...", RETRY_DELAY)
                    time.sleep(RETRY_DELAY)
                else:
                    Logger.critical("[REQUESTS] Não foi possível pegar as informações!")
                    sys.exit()
        return ""

    def _get_schedules(self) -> str:
        """
            Método que coleta as informações de agendamento de atualização.
        """

        self._data.setdefault("canceled", {})
        for workspace_object in (objects := json.loads(self.workspace_objects)):
            headers = {
                "Authorization": f"Bearer {self._access_token}" 
            }

            self._data.setdefault(workspace_object, {})
            # self._data[workspace_object].setdefault("dataflows", {})
            self._data[workspace_object].setdefault("datasets", {})

            self._canceled.setdefault(workspace_object, {})
            # self._canceled[workspace_object].setdefault("dataflows", [])
            self._canceled[workspace_object].setdefault("datasets", [])

        #     ATUALMENTE DESATIVADO: API NÃO TEM GET REFRESH SCHEDULES DE DATAFLOWS 😢

        #     for dataflow in (dataflows := objects[workspace_object]['dataflows']):
        #         self._data[workspace_object]["dataflows"].setdefault(dataflow, {})

        #         Logger.info("Pegando informações do dataflow %s", dataflow)
        #         try:
        #             dataflow_url = (
        #                 BASE_URL
        #                 + f"/{quote(objects[workspace_object]['workspace_id'], safe='/-')}"
        #                 + f"/dataflows/{quote(dataflows[dataflow], safe='/-')}/refreshSchedule"
        #             )
        #             dataflow_request = requests.get(
        #                 url=dataflow_url,
        #                 headers=headers,
        #                 timeout=TIMEOUT
        #             )

        #             if dataflow_request.status_code == 404:
        #                 Logger.warning(
        #                     "Agendamento não realizado / cancelado. Dataflow: %s. Workspace: %s.",
        #                     workspace_object, dataflow
        #                 )
        #                 self._canceled[workspace_object]["dataflows"].append(dataflow)
        #                 continue

        #             dataflow_request.raise_for_status()
        #             dataflow_data = dataflow_request.json()

        #             self._data[workspace_object]["dataflows"][dataflow] = {
        #                 "days": dataflow_data.get("days", "none"),
        #                 "times": dataflow_data.get("times", "none"),
        #                 "enabled": dataflow_data.get("enabled", "none"),
        #             }

        #             time.sleep(REQUEST_INTERVAL)
        #         except RequestException as error:
        #             if error.response.status_code == 403:
        #                 Logger.warning(
        #                     "[REQUESTS] Dataflow - "
        #                     "Acesso negado ao dataflow %s na workspace %s."
        #                     "Tentando pegar novo access token...",
        #                     dataflow, workspace_object
        #                 )
        #                 self._access_token = login(SCOPE)
        #                 return self._get_schedules()
        #             Logger.error("[REQUESTS] Dataflow %s: %s", dataflow, error)

            for dataset in (datasets := objects[workspace_object]['datasets']):
                self._data[workspace_object]["datasets"].setdefault(dataset, {})

                Logger.info("Pegando informações do dataset %s", dataset)
                try:
                    dataset_url = (
                        BASE_URL
                        + f"/{quote(objects[workspace_object]['workspace_id'], safe='/-')}"
                        + f"/datasets/{quote(datasets[dataset], safe='/-')}/refreshSchedule"
                    )
                    dataset_request = requests.get(
                        url=dataset_url,
                        headers=headers,
                        timeout=TIMEOUT
                    )

                    if dataset_request.status_code == 404:
                        Logger.warning(
                            "Agendamento não realizado / cancelado. Dataset: %s. Workspace: %s.",
                            workspace_object, dataset
                        )
                        self._canceled[workspace_object]["datasets"].append(dataset)
                        continue

                    dataset_request.raise_for_status()
                    dataset_data = dataset_request.json()

                    self._data[workspace_object]["datasets"].update(
                        {
                            "name": dataset,
                            "days": dataset_data.get("days", "none"),
                            "times": dataset_data.get("times", "none"),
                            "enabled": dataset_data.get("enabled", "none"),
                        }
                    )

                    time.sleep(REQUEST_INTERVAL)
                except RequestException as error:
                    if error.response.status_code == 403:
                        Logger.warning(
                            "[REQUESTS] Dataset - "
                            "Acesso negado ao dataset %s na workspace %s." \
                            "Tentando pegar novo access token...",
                            dataset, workspace_object
                        )
                        self._access_token = login(SCOPE)
                        return self._get_schedules()
                    Logger.error("[REQUESTS] Dataset %s: %s", dataset, error)
        self._data["canceled"].update(self._canceled)
        return json.dumps(self._data, indent=4)

    @property
    def workspaces(self) -> str:
        """
            Property that gets all the workspaces.
        """

        if not self._workspaces:
            self._workspaces = self._get_workspaces()
        return self._workspaces

    @property
    def workspace_objects(self) -> str:
        """
            Property that gets all the workspace objects - datasets and dataflows.
        """

        if not self._objects:
            self._objects = self._get_w_objects()
        return self._objects

    @property
    def schedules(self) -> str:
        """
            Property that gets all the schedules from the datasets.
        """

        if not self._data:
            self._data = self._get_schedules()
        return self._data

    def get_info(self) -> dict:
        """
            Método que gerencia toda a classe.
            Faz login quando necessário, pega as workspaces e coleta dos dados.
        """

        return json.loads(self.schedules)
    
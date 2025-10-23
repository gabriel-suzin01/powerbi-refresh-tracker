"""
    Módulo com funções e variáveis usadas em outras partes do projeto.

    Inclui:
    - Funções para autenticação e obtenção de tokens de acesso.
    - Funções para interagir com a UI usando SELENIUM.
    - Configuração de logging para monitoramento e depuração.
"""

import sys
import time
import requests
from requests.exceptions import RequestException
from dotenv import set_key

from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from src.setup import Config, Logger, get_env_path, get_env_values

# Variáveis globais

CHROME_SERVICE = Service(ChromeDriverManager().install())

WEBDRIVER_OPTIONS = webdriver.ChromeOptions()

if Config.get("INIT", "SHOW_SCREEN").lower() == "false":
    WEBDRIVER_OPTIONS.add_argument("--headless=new")
else:
    WEBDRIVER_OPTIONS.add_argument("--start-maximized")

WEBDRIVER_OPTIONS.add_argument("--disable-notifications")
WEBDRIVER_OPTIONS.add_argument("--disable-extensions")
WEBDRIVER_OPTIONS.add_argument("--disable-background-networking")
WEBDRIVER_OPTIONS.add_argument("--disable-gpu")

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 10
WORKSPACE_REQUEST_INTERVAL = 3

# Funções

def get_device_code(tenant_id: str, client_id: str, scope: str) -> dict:
    """
        Obtém o código do dispositivo para autenticação OAuth2.
        Este código posteriormente é processado, para obter um token de acesso.
        
        Parâmetros:
        - tenant_id (str): ID do tenant do Azure AD.
        - client_id (str): ID do cliente (aplicativo registrado no Azure AD).
        - scope (str): É a url que se quer ter acesso, seguido de '.default'.

        OBS.: Para que funcione corretamente, tem que ter as permissões no portal do Azure.
    """

    Logger.info("[REQUESTS] Adquirindo device code...")

    auth_url =  f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/devicecode"
    data = {
        "client_id": client_id,
        "scope": scope
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(url=auth_url, data=data, headers=headers, timeout=TIMEOUT)
    except RequestException as error:
        Logger.error("[REQUESTS] Erro inesperado! Descrição: %s", error)

    Logger.info("[REQUESTS] Device code adquirido com sucesso!")
    return response.json()

def get_access_token(driver: webdriver, scope: str) -> str:
    """
        Obtém o token de acesso usando o código do dispositivo.
        Este token é usado para realizar requisições à API do Power BI / Sharepoint.

        Parâmetros:
        - driver (webdriver): Instância do WebDriver do Selenium. É a instância do navegador ativo.
    """

    device_code = get_device_code(
        get_env_values().get('TENANT_ID'),
        get_env_values().get('CLIENT_ID'),
        scope
    )

    Logger.info("[SELENIUM] Adquirindo access token...")
    access_token = None

    driver.get(device_code.get("verification_uri", "#"))

    interact_with_ui(driver=driver, css="[id='otc']", value=device_code["user_code"])

    try:
        css = "[class='table'][role='button'][aria-describedby='tileError loginHeader']"
        interact_with_ui(driver=driver, css=css)
    except WebDriverException:
        try:
            Logger.info("[SELENIUM] Inserindo e-mail...")
            interact_with_ui(
                driver=driver,
                css="[type='email'][id='i0116']",
                value=get_env_values().get('EMAIL')
            )

            Logger.info("[SELENIUM] Inserindo senha...")
            interact_with_ui(
                driver=driver,
                css="[type='password'][id='i0118']",
                value=get_env_values().get('PASSWORD')
            )

            Logger.info("[SELENIUM] Clicando para continuar...")
            interact_with_ui(driver=driver, css="[id='idSIButton9']")

            try:
                Logger.info("[SELENIUM] Clicando em 'continuar' para permanecer conectado...")
                interact_with_ui(driver=driver, css="[id='idSIButton9']")
            except WebDriverException:
                Logger.info("[SELENIUM] Não foi necessário clicar em 'continuar'.")
        except WebDriverException:
            Logger.error("[SELENIUM] Não foi possível prosseguir o fluxo.")

    wait_loading(driver)
    time.sleep(WORKSPACE_REQUEST_INTERVAL)

    token_url = (
        f"https://login.microsoftonline.com/{get_env_values().get('TENANT_ID')}"
        "/oauth2/v2.0/token"
    )
    token_data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "client_id": get_env_values().get('CLIENT_ID'),
        "device_code": device_code["device_code"]
    }

    expires_in = device_code["expires_in"]
    interval = device_code.get("interval", 5)

    start_time = time.time()

    while time.time() - start_time < expires_in:
        try:
            token_response = requests.post(token_url, data=token_data, timeout=TIMEOUT)
            token_json = token_response.json()

            if "access_token" in token_json:
                access_token = token_json["access_token"]
                break

            if token_json.get("error"):
                raise RequestException
        except RequestException as error:
            Logger.error("[REQUESTS] Erro inesperado: %s", error)

        time.sleep(interval)

    if not access_token:
        Logger.critical("[REQUESTS] Não foi possível obter o token de acesso!")
        sys.exit()

    if "sharepoint" in scope:
        set_key(get_env_path(), "ACCESS_TOKEN_SHAREPOINT", access_token)
    else:
        set_key(get_env_path(), "ACCESS_TOKEN", access_token)
    Logger.info("[REQUESTS] Access token adquirido com sucesso!")

    return access_token

def handle_request_exception(attempt: int, error: str, scope: str) -> str:
    """
        This function handles the request exception in the modules 'sharepoint' and 'info'
    """
    Logger.error("[REQUESTS] Tentativa %s. Erro: %s", attempt, error)
    if attempt == 1:
        Logger.info("[REQUESTS] Chamando função para adquirir novo access token...")
        return login(scope)
    if attempt < MAX_RETRIES:
        Logger.info("[REQUESTS] Tentando novamente em %s segundos...", RETRY_DELAY)
        time.sleep(RETRY_DELAY)
    else:
        Logger.critical("[REQUESTS] Não foi possível pegar as workspaces!")
        sys.exit()
    return ""

def wait(driver: webdriver) -> WebDriverWait:
    """
        Método usado somente para reduzir o método WebDriverWait.

        Parâmetros:
        - driver (webdriver): Instância do navegador a esperar.
    """
    return WebDriverWait(driver, TIMEOUT)

def interact_with_ui(driver: webdriver, css: str, value = None) -> None:
    """
        Função usada para interagir com a UI da página.
        Pode ser usada para clicar em botões ou inserir valores em campos de texto.

        Parâmetros:

        - driver (webdriver): Instância do WebDriver do Selenium. É a instância do navegador ativo.
        - css (str): Seletor CSS do elemento com o qual se quer interagir.
        - value (str, opcional): Valor a ser inserido no input. Se vazio, o elemento é clicado.
    """

    selector = (By.CSS_SELECTOR, css)

    Logger.debug("[SELENIUM] Interagindo com elementos da tela...")

    if value:
        wait(driver).until(EC.visibility_of_element_located(selector))
        element = driver.find_element(*selector) # o '*' desempacota a tupla
        element.send_keys(value)
        element.send_keys(Keys.RETURN)
    else:
        wait(driver).until(EC.visibility_of_element_located(selector))
        element = driver.find_element(*selector) # o '*' desempacota a tupla
        element.click()

def wait_loading(driver: webdriver) -> None:
    """
        Função que espera o carregamento completo da página.
        Útil para garantir que a página esteja totalmente carregada antes de prosseguir.

        Parâmetros:
        - driver (webdriver): Instância do WebDriver do SELENIUM. É a instância do navegador ativo.
    """

    while True:
        state = driver.execute_script("return document.readyState")

        if state == "complete":
            break

def login(scope: str) -> str:
    """
        Função responsável por fazer login e conseguir o access_token.
    """

    for attempt in range(1, MAX_RETRIES + 1, 1):
        try:
            driver = webdriver.Chrome(service=CHROME_SERVICE, options=WEBDRIVER_OPTIONS)

            return get_access_token(driver=driver, scope=scope)
        except WebDriverException as error:
            Logger.error("[SELENIUM] Tentativa %s. Erro: %s", attempt, error)
            if attempt < MAX_RETRIES:
                Logger.info("[SELENIUM] Tentando novamente em %s segundos...", RETRY_DELAY)
                time.sleep(RETRY_DELAY)
            else:
                Logger.critical("[SELENIUM] Não foi possível fazer login!")
                sys.exit()
        finally:
            if driver:
                driver.quit()
    return ""

"""
    Módulo que realiza o setup da .env, e de outras funcionalidades básicas.
    
    Inclui:
    - Funções para configuração básica da biblioteca logging.
    - Funções para pegar os valores da .env.
    - Funções para inserir valores na .env.
"""

import configparser
import os
import sys
import logging
from pathlib import Path
from tkinter import messagebox

import ttkbootstrap as ttk
from ttkbootstrap.constants import SUCCESS

from dotenv import dotenv_values, load_dotenv, set_key

# Configurando logging

logging.basicConfig(
    level=logging.INFO, # dá de reduzir, ou aumentar nível de logging
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logger.log",
    filemode="w"
)

Logger = logging.getLogger()

# Configurando path do .env e .ini

Config = configparser.ConfigParser()

if getattr(sys, 'frozen', False): # atributo criado pelo pyinstaller
    CONFIG_BASE_PATH = Path(sys.executable).parent / "settings.ini"
    ENV_PATH = Path(sys.executable).parent / ".env"
else:
    CONFIG_BASE_PATH = Path(__file__).parent.parent / "settings.ini"
    ENV_PATH = Path(__file__).parent.parent / ".env"

Config.read(CONFIG_BASE_PATH)

def get_env_path() -> Path:
    """
        Função que retorna o PATH da .env.
    """
    return ENV_PATH

def env_has_values() -> bool:
    """
        Função que analisa se a .env já tem valores ou não.
    """

    if (env_vars := dotenv_values(ENV_PATH)):
        for _, item in env_vars.items():
            if item.strip() == "":
                return False
        return True
    return False

def get_env_values() -> dict[str, str]:
    """
        Função para atualizar valores das variáveis vindas da .env.
    """

    load_dotenv(override=True)

    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    last_access_token = os.getenv("ACCESS_TOKEN")

    return {
        "TENANT_ID": tenant_id,
        "CLIENT_ID": client_id,
        "EMAIL": email,
        "PASSWORD": password,
        "ACCESS_TOKEN": last_access_token
    }

def insert_env_variables() -> None:
    """
        Função para inserir as variáveis de ambiente.
        Rodam uma vez só (para não parar com automações).
    """

    if not ENV_PATH.exists():
        ENV_PATH.touch()

    if env_has_values():
        return

    app = ttk.Window(title="Informações essenciais", themename="darkly")

    window_width = 500
    window_height = 400
    x = (app.winfo_screenwidth() // 2) - (window_width // 2)
    y = (app.winfo_screenheight() // 2) - (window_height // 2)
    app.geometry(f"{window_width}x{window_height}+{x}+{y}")

    tenant_var = ttk.StringVar()
    client_var = ttk.StringVar()
    email_var = ttk.StringVar()
    password_var = ttk.StringVar()

    ttk.Label(app, text="Tenant ID (Azure)").pack(pady=5)
    ttk.Entry(app, textvariable=tenant_var, width=40).pack()

    ttk.Label(app, text="Client ID (Azure)").pack(pady=5)
    ttk.Entry(app, textvariable=client_var, width=40).pack()

    ttk.Label(app, text="E-mail").pack(pady=5)
    ttk.Entry(app, textvariable=email_var, width=40).pack()

    ttk.Label(app, text="Senha do e-mail").pack(pady=5)
    ttk.Entry(app, textvariable=password_var, width=40, show="*").pack()

    def aparecer_mensagem() -> None:
        messagebox.showwarning(title="Aviso!", message="Preencha todos os campos!")

    def enviar() -> None:
        if any(not var.get().strip() for var in (tenant_var, client_var, email_var, password_var)):
            aparecer_mensagem()
            Logger("[ENV] Erro, valores inseridos são inválidos!")
            return

        set_key(ENV_PATH, "TENANT_ID", tenant_var.get())
        set_key(ENV_PATH, "CLIENT_ID", client_var.get())
        set_key(ENV_PATH, "EMAIL", email_var.get())
        set_key(ENV_PATH, "PASSWORD", password_var.get())

        app.destroy()

    ttk.Button(app, text="Enviar", bootstyle=SUCCESS, command=enviar).pack(pady=20)

    app.protocol("WM_DELETE_WINDOW", sys.exit)
    app.mainloop()

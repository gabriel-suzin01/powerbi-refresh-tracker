"""Módulo principal. Ele que é o responsável pela execução do programa."""

from src.info import WebExtractor
from src.setup import insert_env_variables
from src.sharepoint import UpdateSharepointFile

def main() -> None:
    """Função principal."""

    insert_env_variables()

    infos = WebExtractor()
    json_data = infos.get_info()

    sharepoint = UpdateSharepointFile()
    sharepoint.put_in_sharepoint(json_data)

if __name__ == "__main__":
    main()

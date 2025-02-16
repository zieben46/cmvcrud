import os

from controller.api_controller import APIController
from viewer.cli_viewer import CLIViewer
from configs.db_configs import PostgresConfig

from sqlalchemy import create_engine


# ========== Run Application ==========
if __name__ == "__main__":

    # api_controller = APIController()
    cli_view = CLIViewer()
    
    cli_view.display_menu()

    
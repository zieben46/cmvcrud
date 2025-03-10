from dbadminkit.streamlit.base_app import StreamlitDBApp
from dbadminkit.core.database_profile import DatabaseProfile

class OrdersApp(StreamlitDBApp):
    def __init__(self, config: DatabaseProfile):
        super().__init__(config, app_name="Orders")

    def get_table_info(self) -> dict:
        return {"table_name": "orders", "key": "order_id"}

if __name__ == "__main__":
    config = DatabaseProfile.live_postgres()
    app = OrdersApp(config)
    app.run()
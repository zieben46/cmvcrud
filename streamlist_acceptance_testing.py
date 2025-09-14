import pytest
from streamlit.testing.v1 import AppTest
from streamlit import session_state

# Example Streamlit app structure (for reference, assumed in app.py):
"""
# app.py
import streamlit as st
from orchestrator import Orchestrator
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize orchestrator
orchestrator = Orchestrator(db_type=os.getenv("DB_TYPE"), db_path=os.getenv("DB_PATH"))

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
    st.session_state["spend_plans"] = []
    st.session_state["username"] = "anonymous"

def login_page():
    with st.form("login_form"):
        username = st.text_input("Username (optional)", key="username_input")
        password = st.text_input("Password", type="password", key="password_input")
        if st.form_submit_button("Login", key="login_submit"):
            if password == "correct_password":
                st.session_state["authenticated"] = True
                st.session_state["username"] = username or "anonymous"
                st.success("Login successful!")
            else:
                st.error("Invalid password")

def spend_plan_form():
    if not st.session_state["authenticated"]:
        st.error("Please log in.")
        return
    with st.form("spend_plan_form"):
        amount = st.text_input("Amount", key="spend_plan_amount")
        category = st.text_input("Category", key="spend_plan_category")
        if st.form_submit_button("Submit Spend Plan", key="submit_spend_plan_form"):
            if amount and category and float(amount) > 0:
                st.session_state["spend_plans"].append({"amount": amount, "category": category})
                orchestrator.upsert(st.session_state["spend_plans"])
                st.success("Spend plan created!")
            else:
                st.error("All fields are required")

def spend_plan_editor():
    if not st.session_state["authenticated"]:
        st.error("Please log in.")
        return
    # Sync session_state with orchestrator data
    st.session_state["spend_plans"] = orchestrator.read()
    if st.session_state["spend_plans"]:
        edited_df = st.data_editor(
            st.session_state["spend_plans"],
            key="spend_plan_editor",
            num_rows="dynamic"
        )
        if st.button("Save Changes", key="save_dataframe_changes"):
            try:
                st.session_state["spend_plans"] = edited_df
                orchestrator.upsert(edited_df)
                st.success("Spend plan changes saved!")
            except ValueError as e:
                st.error(str(e))
    else:
        st.write("No spend plans to edit.")

# Routing
page = st.sidebar.selectbox("Page", ["Login", "Create Spend Plan", "Edit Spend Plans"])
if page == "Login":
    login_page()
elif page == "Create Spend Plan":
    spend_plan_form()
elif page == "Edit Spend Plans":
    spend_plan_editor()
"""

# tests/features/spend_plan/test_edit_dataframe_inmemory.py
import pytest
import os
from streamlit.testing.v1 import AppTest
from dotenv import load_dotenv
from orchestrator import Orchestrator

class User:
    """DSL-like interface for user interactions with the Streamlit app."""
    def __init__(self, app_test: AppTest, name: str = None):
        self.app = app_test
        self.name = name or "anonymous"
        self.has_run = False

    def navigates_to(self, page: str):
        self.app.set_page(page).run()
        self.has_run = True
        return self

    def logs_in_with(self, password: str, username: str = None):
        if not self.has_run:
            self.navigates_to("/login")
        if username:
            self.app.text_input(key="username_input").set_value(username).run()
        self.app.text_input(key="password_input").set_value(password).run()
        self.app.button(key="login_submit").click().run()
        return self

    def sees_login_success(self, expected_message: str):
        assert expected_message in self.app.success[0].value
        return self

    def edits_dataframe(self, resource_type: str, row_index: int, new_data: dict):
        self.navigates_to(f"/{resource_type}_editor")
        current_data = self.app.session_state.get(f"{resource_type}s", [])
        if row_index < len(current_data):
            current_data[row_index].update(new_data)
            self.app.session_state[f"{resource_type}s"] = current_data
            self.app.run()
        self.app.button(key=f"save_{resource_type}_changes").click().run()
        return self

    def sees_resource_updated(self, expected_message: str):
        assert expected_message in self.app.success[0].value
        return self

    def sees_error(self, expected_message: str):
        assert expected_message in self.app.error[0].value
        return self

class Driver:
    """Orchestrates tests by setting up the app and in-memory database."""
    def __init__(self, app_path: str = "app.py", env_file: str = "test.env"):
        load_dotenv(env_file)  # Load test.env for in-memory database
        self.app_test = AppTest.from_file(app_path)
        self.orchestrator = Orchestrator(
            db_type=os.getenv("DB_TYPE"),
            db_path=os.getenv("DB_PATH")
        )
        self.reset_app()

    def as_user(self, name: str = None) -> User:
        return User(self.app_test, name)

    def reset_app(self):
        self.app_test.session_state.clear()
        self.app_test.session_state["authenticated"] = False
        self.app_test.session_state["spend_plans"] = []
        if self.orchestrator.db_type == "inmemory":
            self.orchestrator._data["spend_plans"] = []  # Reset in-memory database
        self.app_test.run()

    def read_spend_plans(self):
        """Retrieve spend plans using orchestrator.read."""
        return self.orchestrator.read()

@pytest.fixture
def driver():
    return Driver()

def test_user_can_edit_spend_plan_via_dataframe_inmemory(driver: Driver):
    # Setup: Pre-populate session_state and in-memory database
    driver.app_test.session_state["spend_plans"] = [{"amount": "100", "category": "groceries"}]
    driver.app_test.session_state["authenticated"] = True
    driver.orchestrator.upsert([{"amount": "100", "category": "groceries"}])

    # When: User edits the dataframe and saves changes
    test_user = driver.as_user("test_user")
    test_user.navigates_to("/spend_plan") \
           .edits_dataframe("spend_plan", row_index=0, new_data={"amount": "150"}) \
           .sees_resource_updated("Spend plan changes saved!")

    # Then: Verify session_state and orchestrator data
    assert driver.app_test.session_state["spend_plans"][0]["amount"] == "150"
    assert driver.read_spend_plans() == [{"amount": "150", "category": "groceries"}]

def test_invalid_amount_in_dataframe_inmemory(driver: Driver):
    # Setup: Pre-populate session_state and in-memory database
    driver.app_test.session_state["spend_plans"] = [{"amount": "100", "category": "groceries"}]
    driver.app_test.session_state["authenticated"] = True
    driver.orchestrator.upsert([{"amount": "100", "category": "groceries"}])

    # When: User tries to edit with an invalid amount
    test_user = driver.as_user("test_user")
    test_user.navigates_to("/spend_plan") \
           .edits_dataframe("spend_plan", row_index=0, new_data={"amount": "-150"}) \
           .sees_error("Invalid amount: must be positive")

    # Then: Verify no changes in session_state or orchestrator data
    assert driver.app_test.session_state["spend_plans"][0]["amount"] == "100"
    assert driver.read_spend_plans() == [{"amount": "100", "category": " workstation"}]

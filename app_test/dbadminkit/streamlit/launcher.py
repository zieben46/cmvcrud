import streamlit as st
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.streamlit.auth import authenticate_user
from dbadminkit.streamlit.apps.employees import EmployeesApp
from dbadminkit.streamlit.apps.orders import OrdersApp

# App registry (expand as needed)
ALL_APPS = {
    "Employees": EmployeesApp,
    "Orders": OrdersApp,
}

# Permissions (e.g., regular users can only access specific apps)
USER_PERMISSIONS = {
    "user1": ["Orders"],  # user1 can only see Orders
}

def main():
    st.title("DBAdminKit Dashboard")

    # Session state for authentication
    if "user" not in st.session_state:
        st.session_state.user = None

    # Login screen
    if not st.session_state.user:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            user_info = authenticate_user(username, password)
            if user_info:
                st.session_state.user = user_info
                st.success(f"Welcome, {username}!")
            else:
                st.error("Invalid credentials")
        return

    # Post-login: App selection
    user = st.session_state.user
    available_apps = ALL_APPS if user["role"] == "admin" else {
        app_name: app_class for app_name, app_class in ALL_APPS.items()
        if app_name in USER_PERMISSIONS.get(user["username"], [])
    }

    if not available_apps:
        st.warning("No apps available for your role.")
        return

    app_name = st.sidebar.selectbox("Select App", list(available_apps.keys()))
    config = DatabaseProfile.live_postgres()  # Adjust as needed
    app = available_apps[app_name](config)
    app.run()

if __name__ == "__main__":
    main() 
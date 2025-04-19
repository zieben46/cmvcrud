# datastorekit/permissions/manager.py
import logging
import bcrypt
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import Session
from CMVCrud.app_store.datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.permissions.models import Users, UserAccess
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class PermissionsManager:
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str):
        """Initialize PermissionsManager for a specific datastore.

        Args:
            orchestrator: DataStoreOrchestrator instance.
            datastore_key: Datastore key (e.g., 'spend_plan_db:safe_user').
        """
        self.orchestrator = orchestrator
        self.datastore_key = datastore_key
        self.adapter = orchestrator.adapters[datastore_key]
        self._initialize_tables()

    def _initialize_tables(self):
        """Create Users and UserAccess tables if they don't exist."""
        try:
            metadata = MetaData()
            Users.__table__.schema = self.adapter.profile.schema if self.adapter.profile.db_type != "sqlite" else None
            UserAccess.__table__.schema = self.adapter.profile.schema if self.adapter.profile.db_type != "sqlite" else None
            metadata.create_all(self.adapter.engine, tables=[Users.__table__, UserAccess.__table__])
            logger.info(f"Initialized Users and UserAccess tables in {self.datastore_key}")
        except Exception as e:
            logger.error(f"Failed to initialize tables: {e}")
            raise

    def add_user(self, username: str, password: str, is_group_admin: bool = False) -> bool:
        """Add a new user with a hashed password.

        Args:
            username: Username for the new user.
            password: Plaintext password to hash.
            is_group_admin: Whether the user is a group admin (default: False).

        Returns:
            bool: True if user added successfully, False otherwise.
        """
        try:
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            with self.adapter.session_factory() as session:
                user = Users(username=username, hashed_password=hashed_password.decode('utf-8'), is_group_admin=is_group_admin)
                session.add(user)
                session.commit()
                logger.info(f"Added user: {username}")
                return True
        except Exception as e:
            logger.error(f"Failed to add user {username}: {e}")
            return False

    def authenticate_user(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate a user by checking username and password.

        Args:
            username: Username to authenticate.
            password: Plaintext password to verify.

        Returns:
            Dict with user details (username, is_group_admin) if authenticated, None otherwise.
        """
        try:
            with self.adapter.session_factory() as session:
                user = session.query(Users).filter_by(username=username).first()
                if user and bcrypt.checkpw(password.encode('utf-8'), user.hashed_password.encode('utf-8')):
                    return {"username": user.username, "is_group_admin": user.is_group_admin}
                logger.warning(f"Authentication failed for user: {username}")
                return None
        except Exception as e:
            logger.error(f"Authentication error for user {username}: {e}")
            return None

    def add_user_access(self, username: str, datastore_key: str, table_name: str, access_level: str) -> bool:
        """Grant access to a user for a specific table.

        Args:
            username: Username to grant access to.
            datastore_key: Datastore key (e.g., 'spend_plan_db:safe_user').
            table_name: Table name (e.g., 'spend_plan').
            access_level: Access level ('read', 'write').

        Returns:
            bool: True if access added successfully, False otherwise.
        """
        try:
            if access_level not in ["read", "write"]:
                raise ValueError(f"Invalid access_level: {access_level}")
            with self.adapter.session_factory() as session:
                access = UserAccess(
                    username=username,
                    datastore_key=datastore_key,
                    table_name=table_name,
                    access_level=access_level
                )
                session.add(access)
                session.commit()
                logger.info(f"Added access for {username} to {datastore_key}.{table_name}: {access_level}")
                return True
        except Exception as e:
            logger.error(f"Failed to add access for {username}: {e}")
            return False

    def check_access(self, username: str, datastore_key: str, table_name: str, operation: str) -> bool:
        """Check if a user has permission for a specific operation on a table.

        Args:
            username: Username to check.
            datastore_key: Datastore key (e.g., 'spend_plan_db:safe_user').
            table_name: Table name (e.g., 'spend_plan').
            operation: Operation to check ('read', 'write').

        Returns:
            bool: True if user has permission, False otherwise.
        """
        try:
            with self.adapter.session_factory() as session:
                # Check if user is group admin
                user = session.query(Users).filter_by(username=username).first()
                if user and user.is_group_admin:
                    return True

                # Check specific access
                access = session.query(UserAccess).filter_by(
                    username=username,
                    datastore_key=datastore_key,
                    table_name=table_name
                ).first()
                if access:
                    if operation == "read" and access.access_level in ["read", "write"]:
                        return True
                    if operation == "write" and access.access_level == "write":
                        return True
                logger.warning(f"No {operation} access for {username} on {datastore_key}.{table_name}")
                return False
        except Exception as e:
            logger.error(f"Access check failed for {username}: {e}")
            return False

    def get_user_access(self, username: str) -> List[Dict]:
        """Get all access permissions for a user.

        Args:
            username: Username to query.

        Returns:
            List of access permissions (datastore_key, table_name, access_level).
        """
        try:
            with self.adapter.session_factory() as session:
                accesses = session.query(UserAccess).filter_by(username=username).all()
                return [
                    {
                        "datastore_key": access.datastore_key,
                        "table_name": access.table_name,
                        "access_level": access.access_level
                    }
                    for access in accesses
                ]
        except Exception as e:
            logger.error(f"Failed to get access for {username}: {e}")
            return []
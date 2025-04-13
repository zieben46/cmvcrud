# TableModel class
class TableModel:
    def __init__(self, table_info: Union[TableInfo, str, Dict]):
        """Initialize with either TableInfo object or JSON/dict representation."""
        if isinstance(table_info, TableInfo):
            self.table_info = table_info
        elif isinstance(table_info, (str, dict)):
            # Handle JSON string or dict
            if isinstance(table_info, str):
                try:
                    data = json.loads(table_info)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON string: {e}")
            else:
                data = table_info
            # Convert to TableInfo (without persisting to DB here)
            self.table_info = TableInfo.from_dict(data)
        else:
            raise TypeError("table_info must be TableInfo, JSON string, or dict")

        # Expose attributes for convenience
        self.table_name = self.table_info.table_name
        self.security_type = self.table_info.security_type
        print(f"Initialized TableModel for {self.table_name} with security {self.security_type}")

    def get_info(self) -> TableInfo:
        """Return the TableInfo object."""
        return self.table_info

    def to_json(self) -> str:
        """Serialize TableInfo to JSON."""
        return json.dumps(self.table_info.to_dict())

# Database setup and usage example
def setup_database(db_url: str):
    """Create database and tables."""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine

def store_table_info(table_info: TableInfo, session):
    """Store TableInfo in database."""
    try:
        session.add(table_info)
        session.commit()
        print(f"Stored TableInfo with ID {table_info.id}")
        return table_info.id
    except Exception as e:
        session.rollback()
        print(f"Error storing TableInfo: {e}")
        raise

def retrieve_table_info(table_id: int, session) -> TableInfo:
    """Retrieve TableInfo by ID."""
    try:
        table_info = session.query(TableInfo).filter_by(id=table_id).first()
        if not table_info:
            raise ValueError(f"No TableInfo found for ID {table_id}")
        return table_info
    except Exception as e:
        print(f"Error retrieving TableInfo: {e}")
        raise

# Example usage
if __name__ == "__main__":
    # Database connection (replace with your PostgreSQL URL)
    db_url = "postgresql://your_user:your_password@localhost:5432/your_database"
    engine = setup_database(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create a TableInfo instance
    table_info = TableInfo(
        table_name="user_data",
        security_type="restricted",
        schema="app",
        description="Stores user information",
        metadata={"owner": "admin"}
    )

    # Store in database
    table_id = store_table_info(table_info, session)

    # Initialize TableModel with TableInfo object
    model1 = TableModel(table_info)
    print(f"Model1 JSON: {model1.to_json()}")

    # Initialize TableModel with JSON string
    json_str = json.dumps(table_info.to_dict())
    model2 = TableModel(json_str)
    print(f"Model2 table name: {model2.table_name}")

    # Initialize TableModel with dict
    dict_data = table_info.to_dict()
    model3 = TableModel(dict_data)
    print(f"Model3 security type: {model3.security_type}")

    # Retrieve from database and initialize TableModel
    retrieved_info = retrieve_table_info(table_id, session)
    model4 = TableModel(retrieved_info)
    print(f"Model4 description: {model4.get_info().description}")

    # Cleanup
    session.close()
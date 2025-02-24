

# db_model ->
# ReflectedTableModel


from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Create the engine first
engine = create_engine('sqlite:///example.db')  # Replace with your database URL

# Define the base
Base = declarative_base()

# Define the class *after* the engine is created
class Master_Table(Base):
    __tablename__ = 'users'
    # Use the engine defined above
    __table_args__ = {'autoload': True, 'autoload_with': engine}
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    created_at = Column(DateTime)


    # example use case
from sqlalchemy.orm import Session

session = Session(engine)

# Query the table
users = session.query(Master_Table).all()
for user in users:
    print(user.id, user.username, user.email, user.created_at)

session.close()
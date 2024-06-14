from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def create_session():
    # Create an engine to connect to a PostgreSQL database
    engine = create_engine('postgresql://joe@/diss', echo=True)
    
    # Create a base class for declarative class definitions
    Base = declarative_base()

    # Create the tables in the database
    Base.metadata.create_all(engine)

    # Create a session maker bound to the engine
    Session = sessionmaker(bind=engine)
    
    # Return a session object
    return Session()

def close_session(session):
    # Close the session
    session.close()


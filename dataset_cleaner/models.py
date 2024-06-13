from sqlalchemy import Column, Integer, DateTime, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import JSONB  # For JSONB type
from datetime import datetime  # For datetime.now()

class Base(DeclarativeBase):
    pass
    
# Define a class representing a table in the database
class Usenet(Base):
    __tablename__ = 'usenet'

    id = Column(Integer, primary_key=True)
    forum_name = Column(Text)
    post_date = Column(DateTime, default=datetime.now)
    comment = Column(JSONB)

    def __repr__(self):
        return f"<Usenet(forum_name='{self.forum_name}', post_date='{self.post_date}', comment='{self.comment}')>"

# Define a class representing a table in the database
class Youtube(Base):
    __tablename__ = 'youtube'

    id = Column(Integer, primary_key=True)
    post_date = Column(DateTime, default=datetime.now)
    comment = Column(JSONB)

    def __repr__(self):
        return f"<Youtube(post_date='{self.post_date}', comment='{self.comment}')>"
    
# Define a class representing a table in the database
class Reddit(Base):
    __tablename__ = 'reddit'

    id = Column(Integer, primary_key=True)
    post_date = Column(DateTime, default=datetime.now)
    comment = Column(JSONB)

    def __repr__(self):
        return f"<Reddit(post_date='{self.post_date}', comment='{self.comment}')>"
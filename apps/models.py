from sqlalchemy import Column, Integer, DateTime, Text, Boolean
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import JSONB  # For JSONB type
from datetime import datetime  # For datetime.now()

class Base(DeclarativeBase):
    pass
    
class Usenet(Base):
    __tablename__ = 'usenet'

    id = Column(Integer, primary_key=True)
    forum_name = Column(Text)
    post_date = Column(DateTime, default=datetime.now)
    comment = Column(JSONB)
    has_subjective = Column(Boolean, default=False)  # New column for subjective analysis
    subjective_patterns = Column(JSONB)  # New column for storing subjective patterns

    def __repr__(self):
        return f"<Usenet(id={self.id}, forum_name='{self.forum_name}', post_date='{self.post_date}', comment='{self.comment}', has_subjective={self.has_subjective}, subjective_patterns='{self.subjective_patterns}')>"

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
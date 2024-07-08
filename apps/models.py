from sqlalchemy import Column, Integer, DateTime, Text, Boolean
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import JSONB  

class Base(DeclarativeBase):
    pass
    
class Usenet(Base):
    __tablename__ = 'usenet'

    id = Column(Integer, primary_key=True)
    forum_name = Column(Text)
    post_date = Column(DateTime)
    comment = Column(JSONB)
    has_detection = Column(Boolean, default=False)  
    objective_patterns = Column(JSONB)  
    subjective_patterns = Column(JSONB)
    possessive_patterns = Column(JSONB) 
    
    def __repr__(self):
        return f"<Usenet(id={self.id}, forum_name='{self.forum_name}', post_date='{self.post_date}', comment='{self.comment}', has_detection={self.has_detection}, objective_patterns='{self.objective_patterns}, subjective_patterns='{self.subjective_patterns}'), possessive_patterns ='{self.possessive_patterns}' ')>"
    
# Define a class representing a table in the database
class Reddit(Base):
    __tablename__ = 'reddit'

    id = Column(Integer, primary_key=True)
    post_date = Column(DateTime)
    comment = Column(JSONB)
    has_detection = Column(Boolean, default=False)  
    objective_patterns = Column(JSONB) 
    subjective_patterns = Column(JSONB) 
    possessive_patterns = Column(JSONB) 

    def __repr__(self):
        return f"<Reddit(id={self.id}, forum_name='{self.forum_name}', post_date='{self.post_date}', comment='{self.comment}', has_detection={self.has_detection}, objective_patterns='{self.objective_patterns}', subjective_patterns='{self.subjective_patterns}'), possessive_patterns ='{self.possessive_patterns}' )>"
    
class Test(Base):
    __tablename__ = 'test'

    id = Column(Integer, primary_key=True)
    forum_name = Column(Text, nullable=True)  
    post_date = Column(DateTime)
    comment = Column(JSONB)
    has_detection = Column(Boolean, default=False)
    objective_patterns = Column(JSONB)
    subjective_patterns = Column(JSONB)
    possessive_patterns = Column(JSONB) 

    def __repr__(self):
        return f"<Test(id={self.id}, forum_name='{self.forum_name}', post_date='{self.post_date}', comment='{self.comment}', has_detection={self.has_detection}, objective_patterns='{self.objective_patterns}'), subjective_patterns='{self.subjective_patterns}'), possessive_patterns ='{self.possessive_patterns}' >"
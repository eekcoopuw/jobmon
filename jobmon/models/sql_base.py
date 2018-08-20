from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Move the base class into one file so it is created once, and re-used

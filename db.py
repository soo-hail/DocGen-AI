# TO INTERACT WITH A POSTGRESQL DATABASE(PYTHONIC WAY) - INTERACTING WITH DATABASE IN A OBJECT-ORIENTED WAY. 
# CONNECT, CREATE TABLES, DEFINE RELATIONSHIPS. 
# DEFINING CLASS FOR TABLES MAKING IT EASIER AND FASTER RO WORK WITH DATABASES.
import os
from pgvector.peewee import VectorField
from peewee import PostgresqlDatabase, Model, TextField, ForeignKeyField

from dotenv import load_dotenv

load_dotenv()  # Load environment variables

# INITIALIZE THE DATABASE.
db = PostgresqlDatabase(
    os.getenv('POSTGRES_DB_NAME'),
    user=os.getenv('POSTGRES_DB_USER'),
    password=os.getenv('POSTGRES_DB_PASSWORD'),
    host=os.getenv('POSTGRES_DB_HOST'),
    port=os.getenv('POSTGRES_DB_PORT')
)

# THIS(DOCUMENT) CLASS REPRESENTA A TABLE IN DATABASE - USED TO CREATE TABLE.
class Documents(Model): # MODEL IS A WAY TO REPRESENT A TABLE IN DATABASE.
   name = TextField() # COLUMN IN DOCUMENT-TABLE, TO STORE DOCUMENT NAME.
   class Meta: # TO STORE META-DATA(DB_NAME, TABLE_NAME)
      database = db
      db_table = 'documents'
      
# CREATE TAGS-TABLE IN DATABASE WHICH STORES TAGS.
class Tags(Model):
   name = TextField()
   class Meta:
      database = db
      db_table = 'tags'
      
# TABLE REPRESENTS MANY-TO-MANY RELATIONSHIPS BETWEEN DOCUMENST AND TAGS.
class DocumentTags(Model):
   document_id = ForeignKeyField(Documents, backref="document_tags", on_delete='CASCADE') # Linked Document-Table
   tag_id = ForeignKeyField(Tags, backref="document_tags", on_delete='CASCADE') # Linked Tag-Table
   class Meta:
      database = db
      db_table = 'document_tags'

# TABLE THAT STORES CHUNKS AND THEI RESPECTIVE EMBEDDINGS OF A DOCUMENT.
class DocumentInformationChunks(Model):
   document_id = ForeignKeyField(Documents, backref="document_information_chunks", on_delete='CASCADE') # LINK TO DOCUMENT TABLE.
   # NOTE: 
   # on_delete='CASCADE': if a document is deleted from the Documents table, all related entries in the current table will also be deleted automatically to keep the database consistent. 
   chunk = TextField() # STORES CHUNKS
   embedding = VectorField(dimensions=1536) # STORES EMBEDDINGS OF A CHUNK. 
   class Meta:
      database = db
      db_table = 'document_information_chunks'
      
# CONNECT TO DATABSE AND 
db.connect()
db.create_tables([Documents, Tags, DocumentTags, DocumentInformationChunks])

# EXECUTE SQL COMMANDS ON CONNECTED DATABASE.

# SET OPENAI-KEY IN DATABASE, FOR EMBEDDINGS AND QUERYING
def set_openai_api_key():
    db.execute_sql(
        "set ai.openai_api_key = %s;\nselect pg_catalog.current_setting('ai.openai_api_key', true) as api_key",
        (os.getenv("OPENAI_API_KEY"),)
    )
    
    
def set_diskann_query_rescore(query_rescore: int):
   db.execute_sql(
      "SET diskann.query_rescore = %s",
      (query_rescore,)
   )
#!/usr/bin/env python

import pandas as pd
from os import getcwd, listdir
from os.path import join, isfile, exists, abspath, pardir
from sys import path
import re

##### Configs #####
database = "sdm"
parent_dir = abspath(join(join(getcwd(), pardir), pardir))
data_dir = join(parent_dir, "data")
scripts_dir = join(parent_dir, "src", "scripts")
path.append(scripts_dir)
from connect import Neo4jConnection

driver = Neo4jConnection(uri="bolt://localhost:7687", user=None, pwd=None, database=database)

def run_query(query):
    """
    Basic wrapper around 'driver' object
    """
    return driver.query(query)

def delete_all_nodes():
    query = """
        MATCH(n) DETACH DELETE(n)
    """
    run_query(query=query)
delete_all_nodes()
##### Create Constraints (Optional)


# In[36]:


def create_document_unqiue_constraint():
    """
    Document's `document_id` should be unique
    """    
    query = """
        CREATE CONSTRAINT document_unqiue IF NOT EXISTS FOR (n: Document) REQUIRE (n.document_id) IS UNIQUE
    """
    run_query(query=query)


# In[37]:


create_document_unqiue_constraint()


# __Author__'s `document_id` should be unique

# In[38]:


def create_author_id_unqiue_constraint():    
    query = """
        CREATE CONSTRAINT author_id_unqiue IF NOT EXISTS FOR (n: Author) REQUIRE (n.author_id) IS UNIQUE
    """
    run_query(query=query)


# In[39]:


create_author_id_unqiue_constraint()


# __Keyword__'s `name` should be unique

# In[40]:


def create_keyword_unqiue_constraint():    
    query = """
        CREATE CONSTRAINT keyword_unqiue IF NOT EXISTS FOR (n: Keyword) REQUIRE (n.name) IS UNIQUE
    """
    run_query(query=query)


# In[41]:


create_keyword_unqiue_constraint()


# ### Create Nodes

# Create `Journal` nodes

# In[67]:


def create_journal_nodes():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/journals.csv" AS x
        WITH toInteger(x.year) AS year, x
        CREATE(n: Journal)
        SET n=x, n.year=year
    """
    run_query(query=query)


# In[68]:


create_journal_nodes()


# Create `Affiliation` nodes

# In[69]:


def create_affiliation_nodes():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/affiliations.csv" AS x
        CREATE(n: Affiliation)
        SET n=x
    """
    run_query(query=query)


# In[70]:


create_affiliation_nodes()


# Create `Keyword` nodes

# In[71]:


def create_keyword_nodes():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/keywords.csv" AS x
        CREATE(n: Keyword)
        SET n=x
    """
    run_query(query=query)


# In[72]:


create_keyword_nodes()


# Create `Author` nodes

# In[73]:


def create_author_nodes():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/authors.csv" AS x
        WITH x, toInteger(x.author_id) as author_id
        CREATE (n: Author)
        SET n=x, n.author_id=author_id
    """
    run_query(query=query)


# In[74]:


create_author_nodes()


# Create `Document` nodes

# In[75]:


def create_document_nodes():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/documents.csv" AS x
        WITH x, split(replace(replace(replace(replace(x.author_ids, '[', ''), ']', ''), ' ', ''), "'", ''), ',') AS y
        UNWIND y AS p
        WITH x, collect(toInteger(p)) AS ids, toInteger(x.document_id) AS document_id
        Create (n: Document)
        SET n=x, n.document_id=document_id, n.author_ids=ids
    """
    run_query(query=query)


# In[76]:


create_document_nodes()


# ### Create Relationships

# Create relationship between `Document` and `Author` nodes

# In[77]:


def create_document_author_relation():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/document_author.csv" AS x
        WITH toInteger(x.author_id) AS auth_id, toInteger(x.document_id) AS doc_id
        MATCH (a:Author {author_id: auth_id}), (b:Document {document_id: doc_id})
        CREATE (b)-[r:written_by]->(a)
    """
    run_query(query=query)


# In[78]:


create_document_author_relation()


# Create relationship between `Document` and `Keyword` nodes

# In[79]:


def create_document_keyword_relation():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/document_keyword.csv" AS x
        WITH x, toInteger(x.document_id) AS doc_id
        MATCH (a:Document {document_id: doc_id}), (b:Keyword {name: x.keyword})
        CREATE (a)-[r:has]->(b)
    """
    run_query(query=query)


# In[80]:


create_document_keyword_relation()


# Create relationship between `Author` and `Keyword` nodes

# In[81]:


def create_author_keyword_relation():    
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/author_keyword.csv" AS x
        WITH x, toInteger(x.author_id) AS auth_id
        MATCH (a:Author {author_id: auth_id}), (b:Keyword {name: x.keyword})
        CREATE (a)-[r:has]->(b)
    """
    run_query(query=query)


# In[82]:


create_author_keyword_relation()


# Create relationship between `Author` and `Affiliation` nodes

# In[83]:


def create_author_affiliation_relation():
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/author_affiliation.csv" AS x
        WITH x, toInteger(x.author_id) AS auth_id
        MATCH (a:Author {author_id: auth_id}), (b:Affiliation {name: x.affiliation})
        CREATE (a)-[r:affiliated_with]->(b)
    """
    run_query(query=query)


# In[84]:


create_author_affiliation_relation()


# Create relationship between `Document` and `Journal` nodes

# In[85]:


def create_document_journal_relation():
    query = """
        MATCH (a:Document), (b:Journal)
        where a.source_title=b.name
        CREATE (a)-[r:published_in]->(b)
    """
    run_query(query=query)


# In[86]:


create_document_journal_relation()


# In[ ]:





#!/usr/bin/env python

from os import getcwd
from os.path import join, abspath, pardir
from sys import path

### Update path to include scripts
parent_dir = abspath(join(join(getcwd(), pardir), pardir))
data_dir = join(parent_dir, "data")
scripts_dir = join(parent_dir, "src", "scripts")
path.append(scripts_dir)
from connect import get_driver

# Global variable for handling the connection
uri, database = "bolt://localhost:7687", "sdm"
driver = get_driver(uri=uri, user=None, pwd=None, database=database)

### Helper methods

def run_query(query):
    """
    Basic wrapper around 'driver' object for queries
    """
    return driver.query(query)

def delete_all_nodes():
    """
    Flush all nodes + relations
    """
    query = """
        MATCH(n) DETACH DELETE(n)
    """
    run_query(query=query)

### Methods for creating constraints

def create_document_unqiue_constraint():
    """
    Create constraint for document_id being unique
    """    
    query = """
        CREATE CONSTRAINT document_unqiue IF NOT EXISTS FOR (n: Document) REQUIRE (n.document_id) IS UNIQUE
    """
    run_query(query=query)

def create_author_id_unqiue_constraint(): 
    """
    Create constraint for author_id being unique
    """
    query = """
        CREATE CONSTRAINT author_id_unqiue IF NOT EXISTS FOR (n: Author) REQUIRE (n.author_id) IS UNIQUE
    """
    run_query(query=query)

def create_keyword_unqiue_constraint():
    """
    Create constraint for keyword being unique
    """
    query = """
        CREATE CONSTRAINT keyword_unqiue IF NOT EXISTS FOR (n: Keyword) REQUIRE (n.name) IS UNIQUE
    """
    run_query(query=query)

### Methods for creating nodes

def create_journal_nodes():
    """
    Create all journal nodes
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/journals.csv" AS x
        WITH toInteger(x.year) AS year, x
        CREATE(n: Journal)
        SET n=x, n.year=year
    """
    run_query(query=query)

def create_affiliation_nodes():
    """
    Create all affiliation nodes
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/affiliations.csv" AS x
        CREATE(n: Affiliation)
        SET n=x
    """
    run_query(query=query)

def create_keyword_nodes():
    """
    Create all keyword nodes
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/keywords.csv" AS x
        CREATE(n: Keyword)
        SET n=x
    """
    run_query(query=query)

def create_author_nodes(): 
    """
    Create all author nodes
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/authors.csv" AS x
        WITH x, toInteger(x.author_id) as author_id
        CREATE (n: Author)
        SET n=x, n.author_id=author_id
    """
    run_query(query=query)

def create_document_nodes():
    """
    Create all document nodes
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/documents.csv" AS x
        WITH x, split(replace(replace(replace(replace(x.author_ids, '[', ''), ']', ''), ' ', ''), "'", ''), ',') AS y
        UNWIND y AS p
        WITH x, collect(toInteger(p)) AS ids, toInteger(x.document_id) AS document_id
        Create (n: Document)
        SET n=x, n.document_id=document_id, n.author_ids=ids
    """
    run_query(query=query)

### Methods for creating relationships

def create_document_author_relation():
    """
    Create relationship between documents and authors
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/document_author.csv" AS x
        WITH toInteger(x.author_id) AS auth_id, toInteger(x.document_id) AS doc_id
        MATCH (a:Author {author_id: auth_id}), (b:Document {document_id: doc_id})
        CREATE (b)-[r:written_by]->(a)
    """
    run_query(query=query)

def create_document_keyword_relation():
    """
    Create relationship between documents and keywords
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/document_keyword.csv" AS x
        WITH x, toInteger(x.document_id) AS doc_id
        MATCH (a:Document {document_id: doc_id}), (b:Keyword {name: x.keyword})
        CREATE (a)-[r:has]->(b)
    """
    run_query(query=query)

def create_author_keyword_relation():
    """
    Create relationship between authors and keywords
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/author_keyword.csv" AS x
        WITH x, toInteger(x.author_id) AS auth_id
        MATCH (a:Author {author_id: auth_id}), (b:Keyword {name: x.keyword})
        CREATE (a)-[r:has]->(b)
    """
    run_query(query=query)

def create_author_affiliation_relation():
    """
    Create relationship between authors and affiliations
    """
    query = """
        LOAD CSV WITH HEADERS FROM "file:///Users/mohammadzainabbas/Downloads/sdm/author_affiliation.csv" AS x
        WITH x, toInteger(x.author_id) AS auth_id
        MATCH (a:Author {author_id: auth_id}), (b:Affiliation {name: x.affiliation})
        CREATE (a)-[r:affiliated_with]->(b)
    """
    run_query(query=query)

def create_document_journal_relation():
    """
    Create relationship between document and journals
    """
    query = """
        MATCH (a:Document), (b:Journal)
        where a.source_title=b.name
        CREATE (a)-[r:published_in]->(b)
    """
    run_query(query=query)

def main():
    """
    main function for loading section
    """
    ## Flush everything first (optional)
    delete_all_nodes()

    ## Create all constraints (optional)
    create_document_unqiue_constraint()
    create_author_id_unqiue_constraint()
    create_keyword_unqiue_constraint()

    ## Create all nodes
    create_journal_nodes()
    # create_affiliation_nodes()
    create_keyword_nodes()
    create_author_nodes()
    create_document_nodes()

    ## Create relationships between nodes
    create_document_author_relation()
    create_document_keyword_relation()
    create_author_keyword_relation()
    # create_author_affiliation_relation()
    create_document_journal_relation()

if __name__ == "__main__":
    main()
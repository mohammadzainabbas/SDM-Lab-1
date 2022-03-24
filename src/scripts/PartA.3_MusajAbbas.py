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

### Methods for creating extra nodes

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

### Methods for creating extra relationships

def create_document_author_relation_for_review():
    """
    Create relationship between documents and authors for reviewer
    """
    query = """
        MATCH (a:Author), (d:Document)
        WHERE d.document_type = "Review"
        CREATE (d)-[r:reviewed_by]->(a)
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

def main():
    """
    main function for evolving section
    """
    ## Create extra nodes
    create_affiliation_nodes()
    
    ## Create extra relationships between nodes
    create_document_author_relation_for_review()
    create_author_affiliation_relation()
    
if __name__ == "__main__":
    main()
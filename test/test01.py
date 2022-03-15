from lib2to3.pgen2 import driver
from os import getcwd
from neo4j import GraphDatabase, Session, basic_auth

def connect():
    """
    Connect to Neo4j
    """
    driver = GraphDatabase.driver("bolt://localhost:7878")
    auth = basic_auth()
    session = driver.session()
    return session, auth, driver

def main():
    session, auth, driver = connect()

    pass

if __name__ == '__main__':
    main()
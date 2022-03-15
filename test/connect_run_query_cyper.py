# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 16:12:21 2022

@author: Rifat
"""

from neo4j import __version__ as neo4j_version
print(neo4j_version)


from neo4j import GraphDatabase
class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response

##from another file
import connection

conn = connection.Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="1234")
query_string='''LOAD CSV with headers FROM 'file:///C:/Users/Rifat/Desktop/SDM_Property_Graph/Data/dblp_school.csv' AS row FIELDTERMINATOR ';'  return row limit 30; '''
conn.query(query_string, db='neo4j')

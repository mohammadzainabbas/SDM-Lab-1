from neo4j import GraphDatabase
from pandas import DataFrame

class Neo4jConnection:
    """
    A basic wrapper for connection to Neo4j graph database
    """
    def __init__(self, uri, user, pwd, database):
        try:
            self.__uri, self.__user, self.__pwd, self.__database = uri, user, pwd, database
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver: {}".format(e))
        
    def valid_driver(self):
        return self.__driver is not None

    def close(self):
        if self.valid_driver():
            self.__driver.close()
            self.__driver = None
        
    def __del__(self):
        self.close()
        
    def query(self, query):
        """
        Method used to query Neo4j graph database
        """
        if not self.valid_driver():
            print("Driver not initialized!")
            return
        if self.__database is None:
            print("No database specified")
            return

        session, response = None, None
        try:
            session = self.__driver.session(database=self.__database)
            # response = session.run(query)
            response = list(session.run(query))
        except Exception as e:
            print("Query failed: {}".format(e))
        finally: 
            if session is not None: session.close()
        return response
    
    def query_with_result(self, query, read_query=True, raw=False):
        """
        Return query with results
        """
        def __run_query(tx, query):
            values, result = list(), tx.run(query)
            columns = list(result.keys())
            for _, record in enumerate(result):
                value = record.values()
                if not value: continue
                values.append(value)
            _ = result.consume()
            return values, columns
        with self.__driver.session(database=self.__database) as session:
            __method = session.read_transaction if read_query else session.write_transaction
            __data, __columns = __method(__run_query, query)
            return __data if raw else DataFrame(__data, columns=__columns)

def get_driver(**kwargs):
    """
    Get an instance of the driver
    """
    return Neo4jConnection(**kwargs)
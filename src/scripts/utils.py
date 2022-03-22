from connect import Neo4jConnection

def run_query(driver, query, **kwargs):
    """
    Basic wrapper around 'driver' object
    """
    if (not isinstance(driver, Neo4jConnection)) or (not isinstance(query, str)): return
    return driver.query(query, **kwargs)

def run_query_with_result(driver, query, **kwargs):
    """
    Basic wrapper around 'driver' object
    """
    if (not isinstance(driver, Neo4jConnection)) or (not isinstance(query, str)): return
    return driver.query_with_result(query, **kwargs)

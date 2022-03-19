// Document's identifier is DOI
CREATE CONSTRAINT document_unqiue IF NOT EXISTS FOR (n: Document) REQUIRE (n.doi) IS UNIQUE;

// Author's identifier is author_id
CREATE CONSTRAINT author_id_unqiue IF NOT EXISTS FOR (n: Author) REQUIRE (n.author_id) IS UNIQUE;

// Keyword's name should be unique
CREATE CONSTRAINT keyword_unqiue IF NOT EXISTS FOR (n: Keyword) REQUIRE (n.name) IS UNIQUE;

// Keyword's name should be unique
CREATE CONSTRAINT keyword_unqiue IF NOT EXISTS FOR (n: Keyword) REQUIRE (n.name) IS UNIQUE;

// For creating Nodes
LOAD CSV WITH HEADERS FROM "file:///<>.csv" AS x
CREATE (n: <>)
SET n = x

// For creating Relationships
MATCH (n: <>), (m: <>)
WHERE <condition>
CREATE (n)-[r:<relation-name>]->[m]
RETURN type(r)


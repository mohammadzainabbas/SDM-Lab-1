// Document's identifier is DOI
CREATE CONSTRAINT document_unqiue IF NOT EXISTS FOR (n: Document) REQUIRE (n.doi) IS UNIQUE;

// Author's identifier is author_id
CREATE CONSTRAINT author_id_unqiue IF NOT EXISTS FOR (n: Author) REQUIRE (n.author_id) IS UNIQUE;

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

// Query - 1
MATCH (d:Document)-[r:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0
WITH j.name AS conference, d.title AS paper, sum(toInteger(d.cited_count)) AS cited_count
ORDER BY cited_count DESC
RETURN conference, collect(paper)[..3] AS papers, collect(cited_count)[..3] AS total_cited_count

// Query - 2
MATCH (a:Author)<-[p:written_by]-(d:Document)-[r:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper'
WITH j.name AS conference, a.author_id AS author_id, a.name AS author_name, count(p) AS paper_count
ORDER BY author_name
WHERE paper_count > 3
RETURN conference, collect(author_name) AS authors, sum(paper_count) AS total_count

// Query - 4
MATCH (a:Author)<-[r:written_by]-(d:Document)-[p:published_in]->(j:Journal)
with a, d, count(p) AS total_cited
with a, d, total_cited ORDER BY total_cited DESC
with a, count(d) AS total_papers, collect(total_cited) AS list_cited
with a, total_papers, list_cited, [x in range(1, size(list_cited)) WHERE x <= list_cited[x - 1] | [list_cited[x - 1], x] ] AS h_index_list
with *, h_index_list[-1][1] AS h_index
ORDER BY h_index DESC
RETURN a.name AS author, total_papers, list_cited, h_index

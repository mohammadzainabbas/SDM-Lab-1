//=========================
// Query No. 01
//=========================

// WITH ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] AS _data

WITH ['data mining', 'big data', 'pca', 'deep learning', 'machine learning', 'data science', 'data structure', 'data', 'data translation', 'database', 'parallel programming', 'video processing', 'clustering', 'dimensional analysis', 'graph theory', 'meta learning', 'algorithm', 'support vector machine', 'computer vision'] AS _data
WITH apoc.convert.toSet(apoc.coll.flatten([x IN _data | split(x, " ")])) + _data AS data

//=========================
// Query No. 02
//=========================

MATCH (a:Author)-[:has]->(k:Keyword), (d:Document)-[:has]->(k1:Keyword)
WHERE k1.name=k.name and apoc.coll.contains(data, toLower(k.name))
RETURN d.title AS paper, collect(a.name) AS authors, k.name AS keyword

//=========================
// Query No. 03
//=========================

CALL gds.pageRank.stream('documents')
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS paper, score
MATCH (paper)-[:published_in]->(:Journal)
WHERE paper.document_type='Conference Paper'
WITH paper.title as title, score ORDER BY score DESC
RETURN collect(title)
LIMIT 100

//=========================
// Query No. 04
//=========================

MATCH (a:Author)<-[:written_by]-(d:Document)
WHERE d.title IN paper
WITH a.name AS author, count(d) AS total_papers
WHERE total_papers >= 2
RETURN collect(author) AS guru_authors

//=========================
// Query No. 01
//=========================

// WITH ['data mining', 'big data', 'pca', 'deep learning', 'machine learning', 'data science', 'data structure', 'data', 'data translation', 'database', 'parallel programming', 'video processing', 'clustering', 'dimensional analysis', 'graph theory', 'meta learning', 'algorithm', 'support vector machine', 'computer vision'] AS _data

WITH ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] AS _data
WITH apoc.convert.toSet(apoc.coll.flatten([x IN _data | split(x, " ")])) + _data AS data

MATCH (a:Author)-[:has]->(k:Keyword), (d:Document)-[:has]->(k1:Keyword)
WHERE k1.name=k.name and apoc.coll.contains(data, toLower(k.name))
RETURN d.title AS paper, collect(a.name) AS authors, k.name AS keyword

//=========================
// Query No. 02
//=========================


//=========================
// Query No. 03
//=========================


//=========================
// Query No. 04
//=========================

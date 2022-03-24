// Create in-memory graph for authors' nodes
CALL gds.graph.create.cypher(
  "authors",
  "Match (a:Author) return id(a) as id",
  "Match (a:Author)<-[r:written_by]-(d:Document) return id(d) as source, id(a) as target",
  {
    validateRelationships
, 
)
YIELD
    graphName AS graph, nodeCount AS nodes, relationshipCount AS rels

///

CALL gds.graph.create('authors',
    'Author',
    {
        CONTRIBUTED: {
            orientation: 'UNDIRECTED'
        }
    }
)

///

CALL gds.betweenness.stream(
    "authors"
)
YIELD
nodeId, score

// Delete in-memory graphs
CALL gds.graph.drop("authors")

/// For 3rd query

MATCH (jo:Journal)
with apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) as all_years
with *, all_years[size(all_years) - 3] as last_year
unwind all_years as year
with year, all_years, apoc.coll.flatten([x in range(0, size(all_years)) where x <= size(all_years) - 3 | 
case when all_years[x + 1]=year - 1 then [all_years[x + 1], all_years[x + 2]] else [] end]) as years
where size(years) >= 1
with *, years[0] as first, years[1] as second
// return year, years, first, second

MATCH (a:Author)<-[r:written_by]-(d:Document)-[p:published_in]->(j:Journal)
with *, a as author, r as written_by, d as document, p as published_in, j as journal 
Call {
    with journal, document, year
    return journal.name as conference, sum(toInteger(document.cited_count)) AS cited_count
}

return conference, cited_count
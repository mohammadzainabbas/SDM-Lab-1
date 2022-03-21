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



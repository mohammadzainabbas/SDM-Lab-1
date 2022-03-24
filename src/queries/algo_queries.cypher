//---------------------------------------------
// Louvain (Community detection algorithm)
//---------------------------------------------
// Create in-memory graph for authors
CALL gds.graph.create.cypher(
  "authors",
  "Match (a:Author) return id(a) as id",
  "Match (a:Author)<-[r:written_by]-(d:Document) return id(d) as source, id(a) as target",
  {validateRelationships: false})
YIELD graphName AS graph, nodeCount AS nodes, relationshipCount AS rels

// Executed algorithm in stream mode 
CALL gds.louvain.stream('authors')
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).name AS author, communityId AS community_id
ORDER BY author

// Write results as property "community"
CALL gds.louvain.write('authors', { writeProperty: 'community' })
YIELD communityCount, modularity, modularities

//---------------------------------------------

//---------------------------------------------
// Strongly) Connected components
//---------------------------------------------
// Create in-memory graph for documents
CALL gds.graph.create('documents',
    'Document',
    {
        reviewed_by: { orientation: 'NATURAL' }
    }
)
// Executed algorithm in stream mode 
CALL gds.alpha.scc.stream("documents")
YIELD nodeId, componentId
RETURN gds.util.asNode(nodeId).title AS paper, componentId as component_id
//---------------------------------------------
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

// Query - 3 (Not correct right now)

MATCH (jo:Journal)
with apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) as all_years
with *, all_years[size(all_years) - 3] as last_year
unwind all_years as year
with year, all_years, apoc.coll.flatten([x in range(0, size(all_years)) where x <= size(all_years) - 3 | 
case when all_years[x + 1]=year - 1 then [all_years[x + 1], all_years[x + 2]] else [] end]) as years
where size(years) >= 1
with *, years[0] as first, years[1] as second
return year, years, first, second

// Query - 4
MATCH (a:Author)<-[r:written_by]-(d:Document)-[p:published_in]->(j:Journal)
with a, d, count(p) AS total_cited
with a, d, total_cited ORDER BY total_cited DESC
with a, count(d) AS total_papers, collect(total_cited) AS list_cited
with a, total_papers, list_cited, [x in range(1, size(list_cited)) WHERE x <= list_cited[x - 1] | [list_cited[x - 1], x] ] AS h_index_list
with *, h_index_list[-1][1] AS h_index
ORDER BY h_index DESC
RETURN a.name AS author, total_papers, list_cited, h_index

///

Match p=(:Author)<-[:written_by]-(:Document)-[:published_in]->(:Journal {name: 'Wind Energy Symposium, 2018'}) return p

///

MATCH (jo:Journal)
with apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) as all_years
with *, all_years[size(all_years) - 3] as last_year

MATCH (d:Document)-[p:published_in]->(j:Journal)
where j.year >= last_year
with *, j.year as year

MATCH (d:Document)-[r:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0
WITH *, j.name AS conference, sum(toInteger(d.cited_count)) AS cited_count
ORDER BY conference DESC
RETURN conference, cited_count
// RETURN conference, all_years, cited_count, year, j.volume

// WITH *, j.name AS conference, apoc.any.properties({year: j.year, volume: j.volume}) as prop, d.document_id as doc, d.title as paper, toInteger(d.cited_count) as cited_count order by cited_count desc

// return conference, prop, paper, sum(cited_count) as cited_count, doc, last_year

// with a, d, count(p) AS total_cited
// with a, d, total_cited ORDER BY total_cited DESC
// return a.name as autho, total_cited

///

MATCH (a:Author)<-[r:written_by]-(d:Document)-[p:published_in]->(j:Journal)
with a, d, count(p) as total_cited
with a, d, total_cited order by total_cited desc
with a, count(d) as total_papers, collect(total_cited) as list_cited
with a, total_papers, list_cited, [x in range(1, size(list_cited)) where x <= list_cited[x - 1] | [list_cited[x - 1], x] ] as h_index_list
with *, h_index_list[-1][1] as h_index
order by h_index desc
RETURN a.name as author, total_papers, list_cited, h_index

///

MATCH (d:Document)-[r:published_in]->(j:Journal)
with j.name as conference, sum(toInteger(d.cited_count)) as citation_count
order by citation_count desc
return conference, citation_count

///

MATCH (jo:Journal)
with apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) as all_years
with *, all_years[size(all_years) - 3] as last_year
unwind all_years as year
with year, all_years, apoc.coll.flatten([x in range(0, size(all_years)) where x <= size(all_years) - 3 | 
case when all_years[x + 1]=year - 1 then [all_years[x + 1], all_years[x + 2]] else [] end]) as years
where size(years) >= 1
with *, years[0] as first, years[1] as second
MATCH (d:Document)-[r:published_in]->(j:Journal)



// MATCH (d:Document)-[p:published_in]->(j:Journal)
// where j.year >= last_year
// with *, j.year as year

// MATCH (d:Document)-[r:published_in]->(j:Journal)
// WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0
// WITH *, j.name AS conference, j.year as year, j.volume as volume, sum(toInteger(d.cited_count)) AS cited_count
// foreach ( x in all_years |
//         With all_years, apoc.coll.indexOf(all_years, x) as i
//         with all_years[i + 1] as first, all_years[i + 2] as second
// )
// return first, second
        // return first, second

    // call apoc.when(x >= last_year, ""
    //     With all_years, apoc.coll.indexOf(all_years, x) as i
    //     with all_years[i + 1] as first, all_years[i + 2] as second
    //     return first, second
        // Match (n:Document)-[p:published_in]->(k:Journal)
        // where x < k.year <= second
        // with k.name as conference, count(p) as _count
        // return conference, _count
    // ""
    // )
    // yield value
// ORDER BY conference DESC
// RETURN result

// RETURN conference, all_years, cited_count, year, j.volume

// WITH *, j.name AS conference, apoc.any.properties({year: j.year, volume: j.volume}) as prop, d.document_id as doc, d.title as paper, toInteger(d.cited_count) as cited_count order by cited_count desc

// return conference, prop, paper, sum(cited_count) as cited_count, doc, last_year

// with a, d, count(p) AS total_cited
// with a, d, total_cited ORDER BY total_cited DESC
// return a.name as autho, total_cited

//====================

MATCH (jo:Journal)
with apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) as all_years
with *, all_years[size(all_years) - 3] as last_year
unwind all_years as year
with year, all_years, apoc.coll.flatten([x in range(0, size(all_years)) where x <= size(all_years) - 3 | 
case when all_years[x + 1]=year - 1 then [all_years[x + 1], all_years[x + 2]] else [] end]) as years
where size(years) >= 1
with {year: year, years: years, first: years[0], second:years[1]} as prop

MATCH (:Author)<-[:written_by]-(d:Document)-[:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0 and prop.year = j.year
WITH prop, {conference: j.name, year:j.year, cited_count: sum(toInteger(d.cited_count))} as cited_prop
// return prop, cited_prop

MATCH (:Author)<-[:written_by]-(:Document)-[p:published_in]->(j:Journal)
WHERE j.year=prop.first and j.name=cited_prop.conference
WITH prop, cited_prop, { conference: j.name, p1: count(p) } as p1_prop

// return prop, cited_prop, p1_prop

MATCH (:Author)<-[:written_by]-(:Document)-[p:published_in]->(j:Journal)
WHERE j.year=prop.second and j.name=cited_prop.conference
WITH prop, cited_prop, p1_prop, { conference: j.name, p2: count(p) } as p2_prop

return cited_prop, p1_prop, p2_prop, prop

// with apoc.map.mergeList([cited_prop, p1_prop, p2_prop]) as _prop
// return _prop
// with prop, apoc.map.merge(cited_prop, p_prop) as pprop
// return pprop



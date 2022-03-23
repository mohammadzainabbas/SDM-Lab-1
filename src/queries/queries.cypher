//=========================
// Query No. 01
//=========================

MATCH (d:Document)-[r:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0
WITH j.name AS conference, d.title AS paper, sum(toInteger(d.cited_count)) AS cited_count
ORDER BY cited_count DESC
RETURN conference, collect(paper)[..3] AS papers, collect(cited_count)[..3] AS total_cited_count

//=========================
// Query No. 02
//=========================

MATCH (a:Author)<-[p:written_by]-(d:Document)-[r:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper'
WITH j.name AS conference, a.author_id AS author_id, a.name AS author_name, count(p) AS paper_count
ORDER BY author_name
WHERE paper_count > 3
RETURN conference, collect(author_name) AS authors, sum(paper_count) AS total_count

//=========================
// Query No. 03
//=========================

MATCH (jo:Journal)
WITH apoc.coll.reverse(apoc.coll.sort(apoc.convert.toSet(collect(jo.year)))) AS all_years
WITH *, all_years[size(all_years) - 3] AS last_year
UNWIND all_years AS year
WITH year, all_years, apoc.coll.flatten([x in range(0, size(all_years)) WHERE x <= size(all_years) - 3 | 
CASE WHEN all_years[x + 1]=year - 1 THEN [all_years[x + 1], all_years[x + 2]] ELSE [] END]) AS years
WHERE size(years) >= 1
WITH {year: year, years: years, first: years[0], second:years[1]} AS prop

MATCH (:Author)<-[:written_by]-(d:Document)-[:published_in]->(j:Journal)
WHERE d.document_type='Conference Paper' AND toInteger(d.cited_count) > 0 and prop.year = j.year
WITH prop, {conference: j.name, year:j.year, cited_count: sum(toInteger(d.cited_count))} AS cited_prop

MATCH (:Author)<-[:written_by]-(:Document)-[p:published_in]->(j:Journal)
WHERE j.year=prop.first and j.name=cited_prop.conference
WITH prop, cited_prop, { conference: j.name, p1: count(p) } AS p1_prop

MATCH (:Author)<-[:written_by]-(:Document)-[p:published_in]->(j:Journal)
WHERE j.year=prop.second and j.name=cited_prop.conference
WITH prop, cited_prop, p1_prop, { conference: j.name, p2: count(p) } AS p2_prop
WITH cited_prop.conference AS conference, (toFloat(cited_prop.cited_count)/toFloat(p1_prop.p1 + p2_prop.p2)) AS impact_factor
ORDER BY impact_factor DESC
RETURN conference, impact_factor

//=========================
// Query No. 04
//=========================

MATCH (a:Author)<-[r:written_by]-(d:Document)-[p:published_in]->(j:Journal)
WITH a, d, count(p) AS total_cited
WITH a, d, total_cited ORDER BY total_cited DESC
WITH a, count(d) AS total_papers, collect(total_cited) AS list_cited
WITH a, total_papers, list_cited, [x in range(1, size(list_cited)) WHERE x <= list_cited[x - 1] | [list_cited[x - 1], x] ] AS h_index_list
WITH *, h_index_list[-1][1] AS h_index
ORDER BY h_index DESC
RETURN a.name AS author, total_papers, list_cited, h_index

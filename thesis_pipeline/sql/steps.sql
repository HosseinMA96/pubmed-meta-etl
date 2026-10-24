--Here I really write things modular!

--Step 1: We are given cen_raw_edges. We decide to discard the seperate nodelist (ignore single nodes).
--The question now is to have a table that maps node_id to doi. Currently that is dimensions.complete node list
-- But we found out it has duplicate dois (one doi for multiple node_ids)
-- Since we decided to work on the PubMed subsection of the network, we do so by inner joining dimensions with
-- our pubmed pipeline (basleine pmids and dois which is stored in hm31.pubmed_etl_mesh_year). This will give us dimensions_joined.
-- To summarize: Step 1: Inner join dimensions.complete node_list on pubmed baselne to get node_id doi pmid_mapping


CREATE TABLE hm31.pmid_limited_dimensions_duplicated as
select edcn.integer_id, edcn.doi, pe.pmid from dimensions.exosome_dimensions_complete_nodelist edcn
                                                   inner join hm31.pubmed_etl_mesh_year pe on pe.doi = edcn.doi
WHERE pe.has_abstract = 1 AND pe.has_title = 1 AND pe.has_mesh = 1 AND pe.has_year = 1; -- 8,599,582 records

-- Step 2: But it turns out we still have pmid repetition.
select count(*) as total, count(distinct(integer_id)) as node_id, count(distinct(pmid)) as pmid from hm31.pmid_limited_dimensions_duplicated;

-- Step 3: Lets assign the pmid to the node with higest degree in the subnetwork, and delete the rest
CREATE TABLE hm31.node_degrees_cert AS
SELECT node_id, COUNT(*) AS degree
FROM (
         -- Use UNION to handle both directions of the undirected edges
         SELECT first AS node_id FROM hm31.cen_intersection_edges_duplicated
         union ALL
         SELECT second AS node_id FROM hm31.cen_intersection_edges_duplicated
     ) AS nodes
GROUP BY node_id;


-- obtain degrees
CREATE TABLE hm31.dimensions_duplicated_with_degrees AS
select pldd.integer_id , pldd.pmid, ndc."degree" , pldd.doi from hm31.pmid_limited_dimensions_duplicated
                                                                     pldd  inner join hm31.node_degrees_cert ndc on pldd.integer_id  = ndc.node_id;

select * from hm31.dimensions_duplicated_with_degrees;

--drop duplicates pmids but keep highest degree
CREATE TABLE hm31.dimensions_unique_cert AS
SELECT integer_id, pmid, doi
FROM (
         SELECT
             integer_id,
             pmid,
             degree,
             doi,
             ROW_NUMBER() OVER (PARTITION BY pmid ORDER BY degree DESC) AS row_num
         FROM hm31.dimensions_duplicated_with_degrees
     ) ranked_pmids
WHERE row_num = 1;


--Now we see it is truly unique pmids node_ids
select count(*) as total, count(distinct(integer_id)) as node_id, count(distinct(pmid)) as pmid from hm31.dimensions_unique_cert;



--Step 4: Grab the sub_network of CEN_raw into a network where both endpoints are within hm31.pmid_limited_dimensions
-- Since we are only interested in Pubmed portion of our network
CREATE TABLE hm31.cen_intersection_edges_cert as
select cre."first", cre."second"  from hm31.cen_raw_edges cre inner join hm31.dimensions_unique_cert pldc1 on
    pldc1.integer_id = cre."first" inner join hm31.dimensions_unique_cert pldc2 on
    pldc2.integer_id = cre."second";
-- 57,512, 086


--Step 3: Take the unique nodes that exist into our network. As a reminder, these are the nodes that have all
-- the features that we are interested in. We will later obtain features for these nodes in a flat table
CREATE TABLE hm31.unique_nodes_cert as
SELECT subquery_alias.node_id, pldc.doi, pldc.pmid
FROM (
         SELECT first AS node_id FROM hm31.cen_intersection_edges_cert ciec
         UNION
         SELECT second AS node_id FROM hm31.cen_intersection_edges_cert ciec
     ) AS subquery_alias
         inner join hm31.dimensions_unique_cert pldc on subquery_alias.node_id = pldc.integer_id
ORDER BY node_id;
-- 7, 445, 898 records

--verify uniqueness of pmids
select count(*) as all_count, count(distinct(node_id)) as node_count, count(distinct(pmid)) as pmid_count from hm31.unique_nodes_cert unc ;


-- Step 4: Now construct the flat feature table. for now it only consists of node_id, year and mesh. Ideally
-- it should have had embeddings as well, but we'll treat that separately
CREATE TABLE hm31.year_mesh_node_features_cert as
select unc.node_id, pemy."year", pemy.mesh  from hm31.unique_nodes_cert unc inner join
                                                 hm31.pubmed_etl_mesh_year pemy on pemy.pmid = unc.pmid;
-- 7, 445, 898 records



-- Step 4: Now we would need to obtain features for mesh and year, for each edge. The much faster approach is to
-- obtain a new edge list where we also have year and mesh for first node and year and mesh for second node as wellCREATE TABLE hm31.year_mesh_node_features_cert as
CREATE TABLE hm31.year_mesh_node_duplicated_edges_cert as
select ciec."first", ymnfc1.year as first_year, ymnfc1.mesh as first_mesh, ciec."second", ymnfc2."year" as second_year,
       ymnfc2.mesh as second_mesh
from hm31.cen_intersection_edges_cert ciec inner join hm31.year_mesh_node_features_cert ymnfc1
                                                      on ciec."first" = ymnfc1.node_id inner join hm31.year_mesh_node_features_cert ymnfc2 on ciec."second" = ymnfc2.node_id;
--57, 512, 086


--we use spark to process our hm31.year_mesh_node_duplicated_edges_cert table to create the wieght
-- for each edge and for each of the two year and mesh cases. It is stored in table below.
-- I ran calculate_similarities.py to obtain it. It took around 1 hour to insert into db ..
select count(*) from hm31.year_mesh_edge_weights_cert ymew;
--57, 512, 086


-- Step 6: We will now create two tables that map node_id to the list of out_neighbors and list of in_neighbors

CREATE TABLE  hm31.out_edges_lookup_cert as
SELECT
    first AS node_id,
    STRING_AGG(second::text, ',') AS out_edges
    FROM
    hm31.cen_intersection_edges_cert ciec
    GROUP BY
    first;

-- 6, 415, 071


CREATE TABLE  hm31.in_edges_lookup_cert as
SELECT
    second AS node_id,
    STRING_AGG(first::text, ',') AS in_edges
    FROM
    hm31.cen_intersection_edges_cert ciec
    GROUP BY
    second;

-- 4,120,853

-- Step 7: As usuall, we have to create a table where for each edge, we have first inward list and second inward list
-- the same also applies for the outward list

CREATE TABLE  hm31.out_edges_features_cert as
select ciec."first" as first, oelc1.out_edges as first_out, ciec."second", oelc2.out_edges as second_out
from hm31.cen_intersection_edges_cert ciec inner join hm31.out_edges_lookup_cert oelc1
on ciec."first" = oelc1.node_id inner join hm31.out_edges_lookup_cert oelc2 on ciec."second" = oelc2.node_id;
-- 53, 200, 439 records. As we expect, some papers don't have any references in our network

CREATE TABLE  hm31.in_edges_features_cert as
select ciec."first" as first, oelc1.in_edges as first_in, ciec."second", oelc2.in_edges as second_in
from hm31.cen_intersection_edges_cert ciec inner join hm31.in_edges_lookup_cert oelc1
on ciec."first" = oelc1.node_id inner join hm31.in_edges_lookup_cert oelc2 on ciec."second" = oelc2.node_id;
-- 44, 509, 707 records
-- This query takes ~ 1hr to complete

-- bib_coupling_edge_weights_cert


-- Step 8: Calculate the bib-coupling  table with spark calculate_similarities.py
select count(*) from hm31.bib_coupling_edge_weights_cert bcewc; -- 53,200,439
select count(*) from hm31.cocitation_edge_weights_cert cewc ; -- 44,509,707
select count(*) from hm31.in_edges_features_cert iefc ;
select count(*) from hm31.out_edges_features_cert oefc ; -- this table deleted since was very large ~240GB

-- The hm31.in_edges_features_cert is around 240 GB and very large. Processing it was impossible without defining an index
--ALTER TABLE hm31.in_edges_features_cert ADD COLUMN id SERIAL PRIMARY KEY;
-- calculate_cocitation_feature.py was used for this


-- ensruing no duplicate edges ..
SELECT first, second, COUNT(*) AS num
FROM hm31.cocitation_edge_weights_cert
GROUP BY first, second
ORDER BY num desc limit 10;





--Step 9: I loaded embeddings into Postgres. The problem is duplicate pmids, ocassionally leading to duplicate rows
select node_id, count(*) as node_count from hm31.uncleaned_embeddings_cert uec group by node_id order by node_count desc limit 10;
-- so lets randomly select one ..

CREATE TABLE hm31.cleaned_embeddings_cert  AS
SELECT DISTINCT ON (node_id)
    node_id,
    embedding
    FROM (
             SELECT
             node_id,
             embedding,
             ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY RANDOM()) AS row_num
    FROM
    hm31.uncleaned_embeddings_cert
    ) RankedRows
    ORDER BY node_id, row_num; -- We have 7,445,837 rows after cleaning. That is slightly lower than unique nodes, 7, 445, 898
-- So I will just proceed

-- Now creating edge-duplicated embeddings. Generates a ~450GB table which is deleted. We will add index for performance
--
CREATE TABLE hm31.embedding_edge_features AS
SELECT
    ROW_NUMBER() OVER () AS id,
        ciec."first",
    ciec."second",
    cec1.embedding AS first_embedding,
    cec2.embedding AS second_embedding
FROM
    hm31.cen_intersection_edges_cert ciec
        LEFT JOIN hm31.cleaned_embeddings_cert cec1 ON ciec."first" = cec1.node_id
        LEFT JOIN hm31.cleaned_embeddings_cert cec2 ON ciec."second" = cec2.node_id;

ALTER TABLE hm31.embedding_edge_features
    ADD PRIMARY KEY (id);



--Step 10: We will use calculate_embedding_feature.py to populate this table
CREATE UNIQUE INDEX idx_unique_pair ON hm31.embedding_edge_weights_cert (first, second);




-- Comment: Turns out 61 papers are missing. Let's find their pmids. I decided to insert null. When calculating cosine similarity
-- If one emnbedding was null, I inserted 0.5
select count(node_id)  from hm31.unique_nodes_cert unc where not exists
                                                                 (SELECT FROM hm31.cleaned_embeddings_cert c WHERE c.node_id = unc.node_id)

SELECT duc.pmid
FROM hm31.unique_nodes_cert unc
         INNER JOIN hm31.dimensions_unique_cert duc ON unc.node_id = duc.integer_id
WHERE NOT EXISTS (
    SELECT *
    FROM hm31.cleaned_embeddings_cert c
    WHERE c.node_id = unc.node_id
);


-- Step 11: Join all feature on each other to obtain a big feature table

--select count(1)

create table hm31.all_edge_features_cert as
select ciec."first", ciec."second", ym.year_similarity, ym.mesh_similarity_mean, ym.mesh_similarity_median,
       cewc.cocitation_jaccard_similarity, cewc.cocitation_frequency_similarity,
       bcewc.bib_coupling_jaccard_similarity, bcewc.bib_coupling_frequency_similarity,
       eewc.cosine_similarity
from hm31.cen_intersection_edges_cert ciec
         left join  hm31.year_mesh_edge_weights_cert ym on ym."first"  = ciec."first" and ym."second" = ciec."second"
         left join hm31.cocitation_edge_weights_cert cewc on cewc."first" = ciec."first" and cewc."second" = ciec."second"
         left join hm31.bib_coupling_edge_weights_cert bcewc on bcewc."first" = ciec."first" and bcewc."second" = ciec."second"
         left join hm31.embedding_edge_weights_cert eewc on eewc."first" = ciec."first" and eewc."second" = ciec."second";a






/* CREATION OF TABLES FOR BULK1 and BULK2 */
CREATE TABLE IF NOT EXISTS bulk1
(
  id character varying(200) NOT NULL,
  firstName character varying(200),
  lastName character varying(200),
  phoneNumber character varying(200),
  addressStreet character varying(200),
  addressState character varying(200), 
  addressCountry character varying(200),
  dateOfBirth character varying(200),
  profilePhoto character varying(200),
  username character varying(200)
);

CREATE TABLE IF NOT EXISTS bulk2
(
  id character varying(200) NOT NULL,
  firstName character varying(200),
  lastName character varying(200),
  phoneNumber character varying(200),
  addressStreet character varying(200),
  addressState character varying(200), 
  addressCountry character varying(200),
  dateOfBirth character varying(200),
  profilePhoto character varying(200),
  username character varying(200)
);

/* COPY FROM CSV FILES TO TABLES */
COPY bulk1 FROM '/Users/ramirodemasi/Devs/wedevelop/ballwiz/code/set-diff/bulk-1.csv' DELIMITER ',' CSV HEADER;
COPY bulk2 FROM '/Users/ramirodemasi/Devs/wedevelop/ballwiz/code/set-diff/bulk-2.csv' DELIMITER ',' CSV HEADER;

/* CREATION OF VIEWS FOR DELETING REPEATED RECORDS WITH SAME ID */
CREATE MATERIALIZED VIEW bulk1_unique AS
SELECT 
    DISTINCT ON (id)
    *
FROM bulk1;

EXPLAIN ANALYZE CREATE MATERIALIZED VIEW bulk2_unique AS
SELECT 
    DISTINCT ON (id)
    *
FROM bulk2;

/* QUERY PLAN                                                            
---------------------------------------------------------------------------------------------------------------------------------
 Unique  (cost=408185.95..414639.10 rows=1290630 width=182) (actual time=43671.614..58027.688 rows=1289760 loops=1)
   ->  Sort  (cost=408185.95..411412.52 rows=1290630 width=182) (actual time=43671.612..57346.554 rows=1290628 loops=1)
         Sort Key: id
         Sort Method: external merge  Disk: 248240kB
         ->  Seq Scan on bulk2  (cost=0.00..47799.30 rows=1290630 width=182) (actual time=34.015..3929.660 rows=1290628 loops=1)
 Planning Time: 68.566 ms
 Execution Time: 101349.993 ms
(7 rows)
*/

/* CREATION OF INDEXES FOR BETTER PERFORMANCE */
CREATE INDEX bulk1_unique_id_idx ON bulk1_unique (id);
CREATE INDEX bulk2_unique_id_idx ON bulk2_unique (id);

/* CREATION OF VIEWS FOR CALCULATING DELTAS */

-- ####### (1) ######
--`ROWS_UPDATED`: RECORDS THAT BOTH TABLES AND THEY HAVE SOME DIFFERENCE IN THE 
-- REST OF THE FIELD.

/* 
  Query: RECORDS THAT BOTH TABLES AND THEY HAVE SOME DIFFERENCE IN THE 
  REST OF THE FIELD.
  Cost: 
  Response Time: 5.46 sec
  CPU: 2.5 GHz Intel Core i5
*/
EXPLAIN ANALYZE CREATE MATERIALIZED VIEW rows_updated AS 
SELECT b1.*
FROM
  bulk1_unique b1
INNER JOIN bulk2_unique b2
ON b1.id = b2.id
WHERE
  (b1.id IS NOT NULL AND 
  b2.id IS NOT NULL) AND 
  ( (b1.firstName != b2.firstName) OR 
    (b1.lastName != b2.lastName) OR 
    (b1.phoneNumber != b2.phoneNumber) OR 
    (b1.addressStreet != b2.addressStreet) OR 
    (b1.addressState != b2.addressState) OR 
    (b1.addressCountry != b2.addressCountry) OR 
    (b1.dateOfBirth != b2.dateOfBirth) OR 
    (b1.profilePhoto != b2.profilePhoto) OR 
    (b1.username != b2.username));

/* OUTPUT: 
 Merge Join  (cost=0.85..209231.70 rows=1289760 width=182) (actual time=0.073..5326.108 rows=1796 loops=1)
   Merge Cond: ((b1.id)::text = (b2.id)::text)
   Join Filter: (((b1.firstname)::text <> (b2.firstname)::text) OR ((b1.lastname)::text <> (b2.lastname)::text) OR ((b1.phonenumber)::text <> (b2.phonenumber)::text) OR ((b1.addressstreet)::text <> (b2.addressstreet)::text) OR ((b1.addressstate)::text <> (b2.addressstate)::text) OR ((b1.addresscountry)::text <> (b2.addresscountry)::text) OR ((b1.dateofbirth)::text <> (b2.dateofbirth)::text) OR ((b1.profilephoto)::text <> (b2.profilephoto)::text) OR ((b1.username)::text <> (b2.username)::text))
   ->  Index Scan using bulk1_unique_id_idx on bulk1_unique b1  (cost=0.43..83283.31 rows=1389022 width=182) (actual time=0.012..1055.973 rows=1389021 loops=1)
         Index Cond: (id IS NOT NULL)
   ->  Index Scan using bulk2_unique_id_idx on bulk2_unique b2  (cost=0.43..77334.23 rows=1289760 width=182) (actual time=0.009..754.215 rows=1289760 loops=1)
         Index Cond: (id IS NOT NULL)
 Planning Time: 120.528 ms
 Execution Time: 5460.967 ms
*/

-- ####### (2) ######

--`ROWS_DELETED`: RECORDS THAT ARE IN BULK1 AND NOT IN BULK2

/* 
  Query: RECORDS THAT ARE IN BULK1 AND NOT IN BULK2
  Cost: 
  Response Time: 116.32 sec
  CPU: 2.5 GHz Intel Core i5
*/
EXPLAIN ANALYZE CREATE MATERIALIZED VIEW rows_deleted AS 
SELECT b1.*
FROM
  bulk1_unique b1
LEFT JOIN bulk2_unique b2
ON b1.id = b2.id
WHERE
  b1.id IS NOT NULL
  AND b2.id IS NULL;

/* OUTPUT:
 Gather  (cost=50600.50..138436.70 rows=99262 width=182) (actual time=38432.576..67248.623 rows=1387226 loops=1)
   Workers Planned: 2
   Workers Launched: 2
   ->  Parallel Hash Anti Join  (cost=49600.50..127510.50 rows=41359 width=182) (actual time=32740.681..68887.721 rows=462409 loops=3)
         Hash Cond: ((b1.id)::text = (b2.id)::text)
         ->  Parallel Seq Scan on bulk1_unique b1  (cost=0.00..43355.59 rows=578759 width=182) (actual time=26.570..370.488 rows=463007 loops=3)
               Filter: (id IS NOT NULL)
         ->  Parallel Hash  (cost=40258.00..40258.00 rows=537400 width=11) (actual time=12094.502..12094.502 rows=429920 loops=3)
               Buckets: 131072  Batches: 32  Memory Usage: 2944kB
               ->  Parallel Seq Scan on bulk2_unique b2  (cost=0.00..40258.00 rows=537400 width=11) (actual time=0.011..154.431 rows=429920 loops=3)
 Planning Time: 320.320 ms
 Execution Time: 116322.969 ms
*/

/* EXTRA QUERY FOR GETTING RECORDS WITH SAME ID IN BULK1 and BULK2 */
SELECT b1.*
FROM
  bulk1_unique b1, bulk2_unique b2
WHERE b1.id = b2.id;

-- ####### (3) ######

--`ROWS_INSERTED`: RECORDS THAT ARE IN BULK2 AND NOT IN BULK1

/* 
  Query: RECORDS THAT ARE IN BULK2 AND NOT IN BULK1
  Cost: 
  Response Time: 55.35354 sec
  CPU: 2.5 GHz Intel Core i5
*/
EXPLAIN ANALYZE CREATE MATERIALIZED VIEW rows_inserted AS 
SELECT b2.*
FROM
  bulk1_unique b1
RIGHT JOIN bulk2_unique b2
ON b1.id = b2.id
WHERE
  b1.id IS NULL
  AND b2.id IS NOT NULL;

/* OUTPUT:
 Gather  (cost=54416.08..126805.43 rows=1 width=182) (actual time=6754.925..17679.153 rows=1287964 loops=1)
   Workers Planned: 2
   Workers Launched: 2
   ->  Parallel Hash Anti Join  (cost=53416.08..125805.33 rows=1 width=182) (actual time=6577.443..19432.802 rows=429321 loops=3)
         Hash Cond: ((b2.id)::text = (b1.id)::text)
         ->  Parallel Seq Scan on bulk2_unique b2  (cost=0.00..40258.00 rows=537400 width=182) (actual time=0.054..353.348 rows=429920 loops=3)
               Filter: (id IS NOT NULL)
         ->  Parallel Hash  (cost=43355.59..43355.59 rows=578759 width=11) (actual time=876.029..876.029 rows=463007 loops=3)
               Buckets: 131072  Batches: 32  Memory Usage: 3104kB
               ->  Parallel Seq Scan on bulk1_unique b1  (cost=0.00..43355.59 rows=578759 width=11) (actual time=0.048..445.132 rows=463007 loops=3)
 Planning Time: 0.298 ms
 Execution Time: 55353.540 ms 
*/



-- #### INFO RELATED TO DATA ANALIZED 

/* 

#BULK1:

- COPIED from csv: 1.389.991
- TOTAL AFTER REMOVED WITH SAME ID: 1.389.022

#BULK2:

- COPIED from csv 1.290.628
- TOTAL AFTER REMOVED WITH SAME ID: 1.289.760


#VIEWS 

- (1) `ROWS_UPDATED`: RECORDS THAT BOTH TABLES AND THEY HAVE SOME 
DIFFERENCE IN THE REST OF THE FIELD
- SIZE = 1796

- (2) `ROWS_DELETED`: RECORDS THAT ARE IN BULK1 AND NOT IN BULK2

- SIZE OF THE VIEW (USING LEFT JOIN) 1.387.226
- DIFF = 1796


- (3) `ROWS_INSERTED`: RECORDS THAT ARE IN BULK2 AND NOT IN BULK1
- SIZE OF THE VIEW (USING RIGHT JOING) 1.287.964
- DIFF= 1796

*/
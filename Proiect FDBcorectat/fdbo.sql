--- Oracle DB Link to Oracle Database Schema

ROLLBACK;

ALTER SESSION CLOSE DATABASE LINK agent_imobiliarDB;
 
DROP DATABASE LINK agent_imobiliarDB;

CREATE DATABASE LINK agent_imobiliarDB

   CONNECT TO agent_imobiliar IDENTIFIED BY agent_imobiliar

   USING '//localhost:1521/XEPDB1';
 
select * from user_db_links;
 
--- Check DB_LINK

select * from user_tables@agent_imobiliarDB;

select * from cities@agent_imobiliarDB;
 
--- Create views on remote tables

DROP VIEW cities_VIEW;

CREATE OR REPLACE VIEW cities_VIEW AS

SELECT id,city_name,state,zip_code 
from cities@agent_imobiliarDB;
SELECT * FROM cities_VIEW;

CREATE OR REPLACE VIEW owners_view AS
SELECT id, full_name, email, phone, created_at
FROM owners@agent_imobiliarDB;

CREATE OR REPLACE VIEW property_types_view AS
SELECT id, type_name, description
FROM property_types@agent_imobiliarDB;

CREATE OR REPLACE VIEW real_estate_view AS
SELECT 
  id, price, beds, baths, house_size, listed_date, is_available,
  city_id, property_type_id, owner_id
FROM real_estate@agent_imobiliarDB;




SELECT HTTPURITYPE.createuri('http://localhost:3000/customers').getclob() as doc 
from dual;

CREATE OR REPLACE VIEW customers_view AS
with rest_doc as
    (SELECT HTTPURITYPE.createuri('http://localhost:3000/customers')
    .getclob() as doc from dual)
SELECT
  id, customer_code, full_name, email, phone, registration_date, country, city as customers
FROM  JSON_TABLE( (select doc from rest_doc) , '$[*]'
            COLUMNS (
                id            PATH '$.id'
                , customer_code     PATH '$.customer_code'
                , full_name           PATH '$.full_name'      
                , email              PATH '$.email'
                , phone            PATH '$.phone'
                , registration_date PATH '$.registration_date'
                , country PATH '$.country'
                , city PATH '$.city'
                )
);
---
SELECT * FROM customers_view;


CREATE OR REPLACE VIEW property_requests_view AS
WITH rest_doc AS (
  SELECT HTTPURITYPE.createuri('http://localhost:3000/property_requests')
  .getclob() AS doc FROM dual)
SELECT
  request_id, customer_id, request_date, property_type, max_budget, min_surface, preferred_city, request_status
FROM JSON_TABLE(
  (SELECT doc FROM rest_doc), '$[*]'
  COLUMNS (
    request_id     PATH '$.request_id',
    customer_id    PATH '$.customer_id',
    request_date   PATH '$.request_date',
    property_type   PATH '$.property_type',
    max_budget     PATH '$.max_budget',
    min_surface    PATH '$.min_surface',
    preferred_city  PATH '$.preferred_city',
    request_status  PATH '$.request_status'
  )
);
SELECT * FROM property_requests_view;

CREATE OR REPLACE VIEW request_feedback_view AS
WITH rest_doc AS (
  SELECT HTTPURITYPE.createuri('http://localhost:3000/request_feedback')
  .getclob() AS doc FROM dual)
SELECT
  id, request_id, feedback_date, rating, comments
FROM JSON_TABLE(
  (SELECT doc FROM rest_doc), '$[*]'
  COLUMNS (
    id                PATH '$.id',
    request_id    PATH '$.request_id',
    feedback_date  PATH '$.feedback_date',
    rating         PATH '$.rating',
    comments       PATH '$.comments'
  )
);
SELECT * FROM request_feedback_view;

--- restheart

CREATE OR REPLACE FUNCTION get_restheart_data_media(pURL VARCHAR2, pUserPass VARCHAR2) 
RETURN clob IS
  l_req   UTL_HTTP.req;
  l_resp  UTL_HTTP.resp;
  l_buffer clob; 
begin
  l_req  := UTL_HTTP.begin_request(pURL);
  UTL_HTTP.set_header(l_req, 'Authorization', 'Basic ' || 
    UTL_RAW.cast_to_varchar2(UTL_ENCODE.base64_encode(UTL_I18N.string_to_raw(pUserPass, 'AL32UTF8')))); 
  l_resp := UTL_HTTP.get_response(l_req);
  UTL_HTTP.READ_TEXT(l_resp, l_buffer);
  UTL_HTTP.end_response(l_resp);
  return l_buffer;
end;
/

SELECT get_restheart_data_media('http://localhost:8081/property_history', 'admin:secret') FROM dual;


-- VIEW: PROPERTY_HISTORY_VIEW_MONGODB
-- Afișează modificările de preț ale proprietăților, data modificării și motivul
CREATE OR REPLACE VIEW PROPERTY_HISTORY_VIEW_MONGODB AS
WITH json AS (
  SELECT get_restheart_data_media('http://localhost:8081/property_history', 'admin:secret') AS doc
  FROM dual
)
SELECT *
FROM JSON_TABLE(
  (SELECT doc FROM json),
  '$[*]'
  COLUMNS (
    mongo_id VARCHAR2(100) PATH '$."_id"."$oid"',
    property_id      VARCHAR2(100) PATH '$.property_id',
    oracle_id          NUMBER        PATH '$.oracle_id',
    previous_price   NUMBER        PATH '$.previous_price',
    new_price        NUMBER        PATH '$.new_price',
    modification_date VARCHAR2(30) PATH '$.modification_date',
    reason           VARCHAR2(100) PATH '$.reason'
  )
);
SELECT * FROM PROPERTY_HISTORY_VIEW_MONGODB;

-- VIEW: MARKET_INSIGHTS_VIEW_MONGODB
-- Afișează media prețurilor și suprafețelor, trendul pieței și orașul aferent
CREATE OR REPLACE VIEW MARKET_INSIGHTS_VIEW_MONGODB AS
WITH json AS (
  SELECT get_restheart_data_media('http://localhost:8081/market_insights', 'admin:secret') AS doc FROM dual
)
SELECT *
FROM JSON_TABLE(
  (SELECT doc FROM json),
  '$[*]'
  COLUMNS (
    mongo_id      VARCHAR2(100) PATH '$."_id"."$oid"',
     oracle_id          NUMBER        PATH '$.oracle_id',
    city          VARCHAR2(100) PATH '$.city',
    average_price NUMBER        PATH '$.average_price',
    average_surface NUMBER      PATH '$.average_surface',
    timestamp     VARCHAR2(30)  PATH '$.timestamp',
    trend         VARCHAR2(50)  PATH '$.trend'
  )
);
SELECT * FROM MARKET_INSIGHTS_VIEW_MONGODB;

-- VIEW: PROPERTY_REVIEWS_VIEW_MONGODB
-- Include comentariile utilizatorilor, scorul acordat și data recenziei
CREATE OR REPLACE VIEW PROPERTY_REVIEWS_VIEW_MONGODB AS
WITH json AS (
  SELECT get_restheart_data_media('http://localhost:8081/property_reviews', 'admin:secret') AS doc FROM dual
)
SELECT *
FROM JSON_TABLE(
  (SELECT doc FROM json),
  '$[*]'
  COLUMNS (
    mongo_id       VARCHAR2(100) PATH '$."_id"."$oid"',
    property_id    VARCHAR2(100) PATH '$.property_id',
    oracle_id      NUMBER         PATH '$.oracle_id',
    review_date    VARCHAR2(30)  PATH '$.review_date',
    rating         NUMBER        PATH '$.rating',
    reviewer       VARCHAR2(100) PATH '$.reviewer',
    comment_text   VARCHAR2(400) PATH '$.comment'
  )
);

SELECT * FROM PROPERTY_REVIEWS_VIEW_MONGODB;

-- VIEW: SITE_ACTIVITY_LOGS_VIEW_MONGODB
-- Monitorizează acțiunile utilizatorilor, IP-ul și momentul accesării platformei
CREATE OR REPLACE VIEW SITE_ACTIVITY_LOGS_VIEW_MONGODB AS
WITH json AS (
  SELECT get_restheart_data_media('http://localhost:8081/site_activity_logs', 'admin:secret') AS doc FROM dual
)
SELECT *
FROM JSON_TABLE(
  (SELECT doc FROM json),
  '$[*]'
  COLUMNS (
    mongo_id  VARCHAR2(100) PATH '$."_id"."$oid"',
    user_id   VARCHAR2(100) PATH '$.user_id',
    oracle_id    NUMBER         PATH '$.oracle_id',
    action    VARCHAR2(100) PATH '$.action',
    timestamp VARCHAR2(30)  PATH '$.timestamp',
    ip_address VARCHAR2(50) PATH '$.ip_address'
  )
);
SELECT * FROM SITE_ACTIVITY_LOGS_VIEW_MONGODB;


-- VIEW-URI DE CONSOLIDARE 

---View 1: request_budget_summary_view (Oracle + PostgreSQL)
-- Oferă un rezumat al cererilor imobiliare agregate pe oraș și țară
-- - Leagă clienții din PostgreSQL (customers_view) cu cererile lor (property_requests_view)
-- - Selectează doar cererile care au bugetul maxim numeric valid
-- - Calculează numărul total de cereri și bugetul mediu maxim pentru fiecare oraș/țară

CREATE OR REPLACE VIEW request_budget_summary_view AS
SELECT 
    c.country,
    c.customers AS city,
    COUNT(*) AS total_requests,
    ROUND(AVG(TO_NUMBER(pr.max_budget)), 2) AS avg_max_budget
FROM 
    property_requests_view pr
JOIN 
    customers_view c ON pr.customer_id = c.id
WHERE 
    REGEXP_LIKE(pr.max_budget, '^\d+(\.\d+)?$') -- doar bugete numerice valide
GROUP BY 
    c.country, c.customers;

SELECT * FROM request_budget_summary_view;


-- VIEW 2: customer_request_summary_view
-- Acest view realizează o consolidare între datele din Oracle și PostgreSQL.
-- Leagă clienții (din `customers_view`, PostgreSQL) de cererile de proprietăți (din `property_requests_view`, PostgreSQL).
-- Pentru fiecare client (`full_name`):
--   - Calculează numărul total de cereri trimise (`total_requests`)
--   - Agregă (concatenează) toate orașele preferate menționate în cereri, într-un singur câmp (`cities_requested`)
-- Rezultatul final oferă o privire generală asupra activității fiecărui client și preferințelor sale geografice.

CREATE OR REPLACE VIEW customer_request_summary_view AS
SELECT 
    c.full_name,
    COUNT(*) AS total_requests,
    LISTAGG(DISTINCT pr.preferred_city, ', ') WITHIN GROUP (ORDER BY pr.preferred_city) AS cities_requested
FROM 
    customers_view c
JOIN 
    property_requests_view pr ON c.id = pr.customer_id
GROUP BY 
    c.full_name;
    
SELECT * FROM customer_request_summary_view;

-- View 3: latest_requests_by_customer_view
-- View-ul arată pentru fiecare client cea mai recentă cerere imobiliară trimisă.
-- Include orașul preferat, tipul proprietății, bugetul și data cererii.
-- Combină date din PostgreSQL (cereri) și Oracle (clienți).

CREATE OR REPLACE VIEW latest_requests_by_customer_view AS
SELECT
    c.full_name,
    pr.request_date,
    pr.preferred_city,
    pr.property_type,
    pr.max_budget
FROM
    customers_view c
JOIN
    property_requests_view pr ON c.id = pr.customer_id
WHERE
    pr.request_date = (
        SELECT MAX(pr2.request_date)
        FROM property_requests_view pr2
        WHERE pr2.customer_id = pr.customer_id
    );
    
SELECT * FROM latest_requests_by_customer_view;


-- View 4: property_price_changes_view (Oracle + MongoDB)
-- Leagă proprietățile din Oracle cu modificările lor de preț din MongoDB
-- Pentru fiecare proprietate, afișează prețul curent, prețul anterior, noul preț, data modificării și motivul modificării.
CREATE OR REPLACE VIEW property_price_changes_view AS
SELECT
    r.id AS property_id,
    r.price AS current_price,
    ph.previous_price,
    ph.new_price,
    ph.modification_date,
    ph.reason
FROM 
    real_estate_view r
JOIN 
    PROPERTY_HISTORY_VIEW_MONGODB ph 
    ON ph.oracle_id = r.id;

SELECT * FROM property_price_changes_view;


-- View 5: property_price_evolution_view (Oracle + MongoDB)
-- Leagă proprietățile din Oracle cu istoricul modificărilor de preț din MongoDB
-- Afișează: ID-ul proprietății, prețul curent, ID-ul orașului, prețul anterior și nou,
-- data modificării și motivul, ordonat după ID-ul proprietății și dată.
CREATE OR REPLACE VIEW property_price_evolution_view AS
SELECT
    r.id AS property_id,
    r.price AS current_price,
    r.city_id,
    ph.previous_price,
    ph.new_price,
    ph.modification_date,
    ph.reason
FROM 
    real_estate_view r
JOIN 
    PROPERTY_HISTORY_VIEW_MONGODB ph 
    ON r.id = ph.oracle_id
ORDER BY
    r.id, ph.modification_date;

SELECT * FROM property_price_evolution_view;


-- View 6: property_review_analysis_view (Oracle + MongoDB)
-- Leagă proprietățile din Oracle cu recenziile din MongoDB (property_reviews)
CREATE OR REPLACE VIEW property_review_analysis_view AS
SELECT 
    r.id AS property_id,
    r.price,
    r.city_id,
    pr.rating,
    pr.review_date,
    pr.reviewer,
    pr.comment_text
FROM 
    real_estate_view r
JOIN 
    PROPERTY_REVIEWS_VIEW_MONGODB pr ON pr.oracle_id = r.id;
    
SELECT * FROM property_review_analysis_view;


-- View 7: site_activity_monitoring_view (Oracle + MongoDB)
-- Leagă utilizatorii din PostgreSQL (customers_view) cu activitatea lor din MongoDB (site_activity_logs)
-- Necesită oracle_id (Int32) în fiecare document din site_activity_logs
CREATE OR REPLACE VIEW site_activity_monitoring_view AS
SELECT 
    c.full_name,
    sa.action,
    sa.timestamp,
    sa.ip_address
FROM 
    customers_view c
JOIN 
    SITE_ACTIVITY_LOGS_VIEW_MONGODB sa ON sa.oracle_id = c.id;

SELECT * FROM site_activity_monitoring_view;


---TABELE FAPTE (ROLAP Fact View)
-- Integrează date cantitative despre modificările de preț ale proprietăților
-- Sursă: Oracle (real_estate_view) + MongoDB (PROPERTY_HISTORY_VIEW_MONGODB)
CREATE OR REPLACE VIEW property_price_fact AS
SELECT
    r.id AS property_id,
    r.price AS current_price,
    ph.previous_price,
    ph.new_price,
    TO_DATE(ph.modification_date, 'YYYY-MM-DD') AS modification_date,
    r.city_id
FROM 
    real_estate_view r
JOIN 
    PROPERTY_HISTORY_VIEW_MONGODB ph ON ph.oracle_id = r.id;

SELECT * FROM property_price_fact;

--Tabele de fapte (ROLAP Fact Views)
-- property_requests_fact (Oracle + MongoDB) 
-- Integrează cererile și feedbackul
CREATE OR REPLACE VIEW property_requests_fact AS
SELECT
    pr.request_id,
    pr.customer_id,
    pr.max_budget,
    pr.min_surface,
    f.rating
FROM 
    property_requests_view pr
LEFT JOIN request_feedback_view f ON f.request_id = pr.request_id;

SELECT * FROM property_requests_fact;

-- VIEW-URI DIMENSIONALE
--Dimensiune extinsă pentru proprietăți.
--Include caracteristici fizice (price, size, beds, etc), locația (oraș, stat) și tipul proprietății. 
--Utilă pentru analize OLAP pe baza locației și a caracteristicilor imobiliare;

CREATE OR REPLACE VIEW dim_properties_extended AS
SELECT 
    r.id AS property_id,
    r.price,
    r.beds,
    r.baths,
    r.house_size,
    r.listed_date,
    r.is_available,
    c.city_name,
    c.state,
    pt.type_name
FROM 
    real_estate_view r
JOIN 
    cities_view c ON r.city_id = c.id
JOIN 
    property_types_view pt ON r.property_type_id = pt.id;

SELECT * FROM dim_properties_extended;


--'Dimensiune pentru clienți. Conține date personale, cod client, locație și dată înregistrare. 
--Folosită pentru analize OLAP privind comportamentul clienților și distribuția geografică.';

CREATE OR REPLACE VIEW dim_customers_enriched AS
SELECT 
    id AS customer_id,
    customer_code,
    full_name,
    email,
    phone,
    registration_date,
    country,
    customers AS city
FROM 
    customers_view;

SELECT * FROM dim_customers_enriched;

--'Dimensiune care îmbină feedbackul primit (rating, comentarii) cu datele cererii imobiliare. 
--Permite analiza calitativă a satisfacției clienților pe oraș sau cerere.';

CREATE OR REPLACE VIEW dim_feedback_rating AS
SELECT 
    rf.id AS feedback_id,
    rf.request_id,
    rf.feedback_date,
    rf.rating,
    rf.comments,
    pr.customer_id,
    pr.preferred_city
FROM 
    request_feedback_view rf
JOIN 
    property_requests_view pr ON rf.request_id = pr.request_id;

SELECT * FROM dim_feedback_rating;

--OLAP
-- View 1: olap_request_summary_view
-- Tip: View OLAP (Oracle + PostgreSQL)
-- Acest view realizează o analiză a cererilor imobiliare agregate pe baza țării și orașului clientului.
-- Se folosesc date integrate din PostgreSQL (property_requests_view, customers_view).
-- Se aplică clauza ROLLUP pentru a obține: totaluri pe fiecare oraș dintr-o țară, totaluri pe fiecare țară, total general la final.

CREATE OR REPLACE VIEW olap_request_summary_view AS
SELECT
    c.country,
    c.customers AS city,
    COUNT(*) AS total_requests,
    ROUND(AVG(TO_NUMBER(pr.max_budget)), 2) AS avg_max_budget
FROM 
    customers_view c
JOIN 
    property_requests_view pr ON c.id = pr.customer_id
WHERE 
    REGEXP_LIKE(pr.max_budget, '^\d+(\.\d+)?$') -- doar bugete valide numeric
GROUP BY 
    ROLLUP (c.country, c.customers);

SELECT * FROM olap_request_summary_view;



-- OLAP VIEW 2: olap_review_summary_view
-- View OLAP care calculează rating-ul mediu și numărul total de recenzii
-- pentru fiecare proprietate din Oracle, pe baza datelor din MongoDB

CREATE OR REPLACE VIEW olap_review_summary_view AS
SELECT
    r.id AS property_id,
    r.price,
    r.city_id,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    COUNT(*) AS total_reviews
FROM 
    real_estate_view r
JOIN 
    PROPERTY_REVIEWS_VIEW_MONGODB pr ON pr.oracle_id = r.id
GROUP BY 
    r.id, r.price, r.city_id;

SELECT * FROM olap_review_summary_view;


-- View OLAP 3: olap_review_summary_rollup_view
--calculează: 
--Rating-ul mediu și numărul total de recenzii
--Agregări pe fiecare proprietate
--Totaluri parțiale pe oraș și un total general
-- Integrează date din Oracle și MongoDB

CREATE OR REPLACE VIEW olap_review_summary_rollup_view AS
SELECT
    r.city_id,                             
    r.id AS property_id,                    
    ROUND(AVG(pr.rating), 2) AS avg_rating, 
    COUNT(*) AS total_reviews               
FROM 
    real_estate_view r
JOIN 
    PROPERTY_REVIEWS_VIEW_MONGODB pr
    ON pr.oracle_id = r.id
GROUP BY 
    ROLLUP (r.city_id, r.id);             

SELECT * FROM olap_review_summary_rollup_view;

-- View OLAP 4: olap_monthly_city_reviews_view
-- Descriere: Afișează media ratingurilor și numărul de recenzii pentru fiecare oraș, lunar
-- Integrare Oracle (real_estate_view, cities_view) + MongoDB (PROPERTY_REVIEWS_VIEW_MONGODB)
-- Agregare: AVG, COUNT
-- Grupare: după lună și oraș (TO_CHAR + city_name)

CREATE OR REPLACE VIEW olap_monthly_city_reviews_view AS
SELECT
    c.city_name,
    TO_CHAR(TO_DATE(pr.review_date, 'YYYY-MM-DD'), 'YYYY-MM') AS review_month,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    COUNT(*) AS total_reviews
FROM 
    real_estate_view r
JOIN 
    PROPERTY_REVIEWS_VIEW_MONGODB pr ON pr.oracle_id = r.id
JOIN
    cities_view c ON r.city_id = c.id
GROUP BY 
    ROLLUP (c.city_name, TO_CHAR(TO_DATE(pr.review_date, 'YYYY-MM-DD'), 'YYYY-MM'))
ORDER BY
    c.city_name, review_month;

SELECT * FROM olap_monthly_city_reviews_view;

-- View OLAP 5: olap_property_type_city_cube_view
-- Scop: Analizează numărul total de cereri imobiliare și bugetul mediu maxim
-- Grupare după tipul de proprietate și oraș (clienți), folosind CUBE pentru a obține toate combinațiile și totalurile
-- Integrează date din PostgreSQL (property_requests_view, customers_view)
CREATE OR REPLACE VIEW olap_property_type_city_cube_view AS
SELECT
    pr.property_type,
    c.customers AS city,
    COUNT(*) AS total_requests,
    ROUND(AVG(TO_NUMBER(pr.max_budget)), 2) AS avg_max_budget
FROM 
    property_requests_view pr
JOIN 
    customers_view c ON pr.customer_id = c.id
WHERE 
    REGEXP_LIKE(pr.max_budget, '^\d+(\.\d+)?$') 
GROUP BY 
    CUBE (pr.property_type, c.customers);

SELECT * FROM olap_property_type_city_cube_view;

-- View OLAP 6: olap_user_activity_summary_view
-- Scop: Analizează activitatea utilizatorilor pe platformă
-- Grupare pe acțiune și oraș, folosind ROLLUP pentru totaluri pe acțiune, pe oraș și total general
-- Integrează date din MongoDB (site_activity_logs) și PostgreSQL (customers_view)

CREATE OR REPLACE VIEW olap_user_activity_summary_view AS
SELECT
    sa.action,                       
    c.customers AS city,           
    COUNT(*) AS total_actions      
FROM 
    SITE_ACTIVITY_LOGS_VIEW_MONGODB sa  
JOIN 
    customers_view c                     
    ON sa.oracle_id = c.id               
GROUP BY 
    ROLLUP(sa.action, c.customers);      

SELECT * FROM olap_user_activity_summary_view;

-- View OLAP 7: olap_feedback_by_city_view
-- Scop: Analizează scorurile medii de feedback pe orașe, pentru cererile de proprietăți
-- Integrează date din PostgreSQL (property_requests_view) și feedback (request_feedback_view)
-- Agregări: AVG (rating), COUNT (feedbacks), folosind ROLLUP pe oraș

CREATE OR REPLACE VIEW olap_feedback_by_city_view AS
SELECT
    pr.preferred_city,                          
    ROUND(AVG(rf.rating), 2) AS avg_rating,     
    COUNT(*) AS total_feedbacks                
FROM 
    property_requests_view pr                   
JOIN 
    request_feedback_view rf                   
    ON pr.request_id = rf.request_id           
GROUP BY 
    ROLLUP (pr.preferred_city);                 

SELECT * FROM olap_feedback_by_city_view;

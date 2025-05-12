
-- DROP TABLES (dacă există deja)
DROP TABLE IF EXISTS request_feedback CASCADE;
DROP TABLE IF EXISTS property_requests CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

GRANT SELECT ON customers TO postgres;
SHOW port;

SELECT COUNT(*) FROM customers;

GRANT USAGE ON SCHEMA public TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO web_anon;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO web_anon;

CREATE ROLE web_anon NOLOGIN;

GRANT USAGE ON SCHEMA public TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO web_anon;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO web_anon;


-- CREARE TABELE
-- Tabela customers
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    customer_code VARCHAR(10),
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    registration_date DATE,
    country VARCHAR(50),
    city VARCHAR(50)
);

-- Tabela property_requests
CREATE TABLE property_requests (
    request_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    request_date DATE,
    property_type VARCHAR(50),
    max_budget NUMERIC(12,2),
    min_surface INTEGER,
    preferred_city VARCHAR(50),
    request_status VARCHAR(20)
);

-- Tabela request_feedback
CREATE TABLE request_feedback (
    id SERIAL PRIMARY KEY,
    request_id INTEGER REFERENCES property_requests(request_id),
    feedback_date DATE,
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    comments TEXT
);

-- VERIFICARE IMPORT

-- Număr clienți pe oraș (după import)
SELECT city, COUNT(*) AS nr_clienti
FROM customers
GROUP BY city;


-- ANALIZE & INTEROGĂRI CLASICE
-- Număr de cereri pe tip de proprietate
SELECT property_type, COUNT(*) AS total_requests
FROM property_requests
GROUP BY property_type;

-- Rating mediu pe oraș (din feedback)
SELECT c.city, ROUND(AVG(f.rating), 2) AS avg_rating
FROM customers c
JOIN property_requests r ON c.id = r.customer_id
JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY c.city;

-- Clienți care au dat feedback slab (rating ≤ 2)
SELECT c.full_name, f.rating, f.comments
FROM customers c
JOIN property_requests r ON c.id = r.customer_id
JOIN request_feedback f ON r.request_id = f.request_id
WHERE f.rating <= 2;

-- Cereri fără feedback
SELECT r.request_id, c.full_name, r.property_type
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
LEFT JOIN request_feedback f ON r.request_id = f.request_id
WHERE f.request_id IS NULL;

-- Buget mediu cerut pe oraș
SELECT preferred_city, ROUND(AVG(max_budget), 2) AS avg_budget
FROM property_requests
GROUP BY preferred_city;

-- Cele mai căutate tipuri de proprietăți per oraș
SELECT preferred_city, property_type, COUNT(*) AS total_requests
FROM property_requests
GROUP BY preferred_city, property_type
ORDER BY preferred_city, total_requests DESC;

-- Orașe în care ratingul mediu este peste 4
SELECT c.city, ROUND(AVG(f.rating), 2) AS avg_rating
FROM customers c
JOIN property_requests r ON c.id = r.customer_id
JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY c.city
HAVING AVG(f.rating) > 4;

-- Clienți care nu au nicio cerere înregistrată
SELECT c.full_name, c.email
FROM customers c
LEFT JOIN property_requests r ON c.id = r.customer_id
WHERE r.request_id IS NULL;

-- Cereri recente (ultimele 15 zile)
SELECT r.request_id, c.full_name, r.property_type, r.request_date
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
WHERE r.request_date >= CURRENT_DATE - INTERVAL '15 days';

--Clienți fără nicio cerere înregistrată
SELECT c.full_name, c.email
FROM customers c
LEFT JOIN property_requests r ON c.id = r.customer_id
WHERE r.request_id IS NULL;

--Cereri înregistrate în ultimele 15 zile
SELECT r.request_id, c.full_name, r.property_type, r.request_date
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
WHERE r.request_date >= CURRENT_DATE - INTERVAL '15 days';

--statistici pe cereri și feedback
SELECT 
  r.preferred_city,
  r.property_type,
  COUNT(f.id) AS feedback_count,
  ROUND(AVG(f.rating), 2) AS avg_rating
FROM property_requests r
LEFT JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY GROUPING SETS (
  (r.preferred_city, r.property_type),
  (r.preferred_city),
  ()
);

-- Afișează doar orașele unde bugetul mediu cerut este mai mare de 80.000
SELECT preferred_city, ROUND(AVG(max_budget), 2) AS avg_budget
FROM property_requests
GROUP BY preferred_city
HAVING AVG(max_budget) > 80000;

--Afișează un clasament al cererilor pe oraș, ordonat descrescător după buget
SELECT r.request_id, r.preferred_city, r.max_budget,
       RANK() OVER (PARTITION BY r.preferred_city ORDER BY r.max_budget DESC) AS budget_rank
FROM property_requests r;

-- OLAP: CUBE și ROLLUP
-- CUBE: combinații posibile oraș + rating
SELECT c.city, f.rating, COUNT(*) AS total_feedback, ROUND(AVG(f.rating), 2) AS avg_rating
FROM customers c
JOIN property_requests r ON c.id = r.customer_id
JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY CUBE (c.city, f.rating);

-- ROLLUP: totaluri cumulative pe oraș + tip proprietate
SELECT preferred_city, property_type, COUNT(*) AS total_requests, ROUND(AVG(max_budget), 2) AS avg_budget
FROM property_requests
GROUP BY ROLLUP(preferred_city, property_type);

-- OLAP: Rating mediu și nr. feedback per oraș, status cerere
SELECT preferred_city, request_status, COUNT(f.id) AS feedback_count, ROUND(AVG(f.rating), 2) AS avg_rating
FROM property_requests r
LEFT JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY CUBE(preferred_city, request_status);

-- OLAP cu HAVING: orașe + tipuri proprietăți cu mai mult de 3 cereri
SELECT preferred_city, property_type, COUNT(*) AS total_requests
FROM property_requests
GROUP BY ROLLUP(preferred_city, property_type)
HAVING COUNT(*) > 3;

-- CUBE cu WHERE: doar cererile de peste 60.000 euro
SELECT preferred_city, request_status, COUNT(*) AS nr, ROUND(AVG(max_budget), 2) AS avg_budget
FROM property_requests
WHERE max_budget > 60000
GROUP BY CUBE(preferred_city, request_status);

-- OLAP cu HAVING: orașe + tipuri proprietăți cu mai mult de 3 cereri
SELECT preferred_city, property_type, COUNT(*) AS total_requests
FROM property_requests
GROUP BY ROLLUP(preferred_city, property_type)
HAVING COUNT(*) > 3;

-- CUBE cu WHERE: doar cererile de peste 60.000 euro
SELECT preferred_city, request_status, COUNT(*) AS nr, ROUND(AVG(max_budget), 2) AS avg_budget
FROM property_requests
WHERE max_budget > 60000
GROUP BY CUBE(preferred_city, request_status);

-- OLAP: GROUPING SETS pentru afișarea cererilor
-- Numărul de cereri pentru fiecare combinație (oraș, status)
-- Totalul cererilor pe fiecare oraș (indiferent de status)
-- Totalul general al cererilor (toate orașele și statusurile)
SELECT preferred_city, request_status, COUNT(*) AS total_requests
FROM property_requests
GROUP BY GROUPING SETS (
  (preferred_city, request_status),
  (preferred_city),
  ()
);

-- View 1: Integrare clienți + cereri + feedback
CREATE OR REPLACE VIEW v_request_feedback_summary AS
SELECT
  c.full_name,
  c.city AS customer_city,
  r.request_id,
  r.property_type,
  r.max_budget,
  r.request_date,
  f.feedback_date,
  f.rating,
  f.comments
FROM
  customers c
JOIN
  property_requests r ON c.id = r.customer_id
LEFT JOIN
  request_feedback f ON r.request_id = f.request_id;

-- View 2: Statistici pe oraș
CREATE OR REPLACE VIEW v_city_stats AS
SELECT
  r.preferred_city,
  COUNT(*) AS total_requests,
  ROUND(AVG(r.max_budget), 2) AS avg_budget,
  COUNT(f.id) AS feedback_count,
  ROUND(AVG(f.rating), 2) AS avg_rating
FROM property_requests r
LEFT JOIN request_feedback f ON r.request_id = f.request_id
GROUP BY r.preferred_city;

-- View 3: Clienți cu scor slab și comentarii
CREATE OR REPLACE VIEW v_negative_feedback_clients AS
SELECT c.full_name, c.email, f.rating, f.comments
FROM customers c
JOIN property_requests r ON c.id = r.customer_id
JOIN request_feedback f ON r.request_id = f.request_id
WHERE f.rating <= 2;

-- View 4: Rezumat buget + suprafață per tip proprietate
CREATE OR REPLACE VIEW v_property_summary AS
SELECT property_type, COUNT(*) AS num_requests,
       ROUND(AVG(max_budget), 2) AS avg_budget,
       ROUND(AVG(min_surface), 2) AS avg_surface
FROM property_requests
GROUP BY property_type;

-- View 5: Clienți înregistrați recent (ultimele 30 zile)
CREATE OR REPLACE VIEW v_recent_customers AS
SELECT *
FROM customers
WHERE registration_date >= CURRENT_DATE - INTERVAL '30 days';

-- View 6: Cereri închise fără feedback
CREATE OR REPLACE VIEW v_closed_requests_no_feedback AS
SELECT r.request_id, r.property_type, c.full_name, r.request_status
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
LEFT JOIN request_feedback f ON r.request_id = f.request_id
WHERE r.request_status = 'Closed' AND f.id IS NULL;

--View 7: Cereri închise care nu au primit feedback
CREATE OR REPLACE VIEW v_closed_requests_no_feedback AS
SELECT r.request_id, r.property_type, c.full_name, r.request_status
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
LEFT JOIN request_feedback f ON r.request_id = f.request_id
WHERE r.request_status = 'Closed' AND f.id IS NULL;

--View 8: Afișează orașele cu cele mai multe cereri de proprietăți
CREATE OR REPLACE VIEW v_top_cities_by_requests AS
SELECT preferred_city, COUNT(*) AS total_requests
FROM property_requests
GROUP BY preferred_city
ORDER BY total_requests DESC;

-- View 9: Cereri cu buget mare și feedback pozitiv (rating ≥ 4)
CREATE OR REPLACE VIEW v_high_budget_positive_feedback AS
SELECT r.request_id, c.full_name, r.max_budget, f.rating
FROM property_requests r
JOIN customers c ON r.customer_id = c.id
JOIN request_feedback f ON r.request_id = f.request_id
WHERE r.max_budget > 90000 AND f.rating >= 4;

-- View 10: Feedback-uri recente în ultimele 20 zile
CREATE OR REPLACE VIEW v_recent_feedback AS
SELECT f.id, c.full_name, r.property_type, f.rating, f.feedback_date
FROM request_feedback f
JOIN property_requests r ON f.request_id = r.request_id
JOIN customers c ON r.customer_id = c.id
WHERE f.feedback_date >= CURRENT_DATE - INTERVAL '20 days';

-- View 11: Cereri grupate după tip și suprafață (medie pe grupă)
CREATE OR REPLACE VIEW v_surface_by_type AS
SELECT property_type, 
       CASE 
         WHEN min_surface < 50 THEN 'Mică'
         WHEN min_surface BETWEEN 50 AND 80 THEN 'Medie'
         ELSE 'Mare'
       END AS surface_group,
       COUNT(*) AS total_requests,
       ROUND(AVG(min_surface), 2) AS avg_surface
FROM property_requests
GROUP BY property_type, surface_group;


SELECT * FROM v_request_feedback_summary LIMIT 10;
SELECT * FROM v_city_stats;
SELECT * FROM v_negative_feedback_clients;
SELECT * FROM v_property_summary;
SELECT * FROM v_closed_requests_no_feedback;
SELECT * FROM v_top_cities_by_requests;
SELECT * FROM v_high_budget_positive_feedback;
SELECT * FROM v_recent_feedback;
SELECT * FROM v_surface_by_type;

--1. Nivel CONSOLIDARE date

-- View_Consolidare_1: Consolidare Clienți și Cereri
CREATE VIEW view_consolidare_customers_requests AS
SELECT 
    c.full_name,
    c.email,
    c.city,
    r.property_type,
    r.request_date,
    r.request_status
FROM 
    customers c
JOIN 
    property_requests r ON c.id = r.customer_id;

-- View_Consolidare_2: Consolidare Cereri și Feedback
CREATE VIEW view_consolidare_requests_feedback AS
SELECT 
    r.request_id,
    r.property_type,
    r.request_date,
    f.rating,
    f.comments
FROM 
    property_requests r
JOIN 
    request_feedback f ON r.request_id = f.request_id;

-- View_Consolidare_3: Consolidare Cereri și Orașe
CREATE VIEW view_consolidare_requests_cities AS
SELECT 
    c.city,
    r.property_type,
    r.request_date,
    r.request_status
FROM 
    customers c
JOIN 
    property_requests r ON c.id = r.customer_id;

-- 2. Schema analitică ROLAP (Tabele de fapte)

-- Tabela_de_fapte_1: Cereri și Feedback
CREATE VIEW view_fact_requests_feedback AS
SELECT 
    r.request_id,
    r.property_type,
    r.request_date,
    COUNT(f.id) AS feedback_count,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM 
    property_requests r
LEFT JOIN 
    request_feedback f ON r.request_id = f.request_id
GROUP BY 
    r.request_id, r.property_type, r.request_date;

-- Tabela_de_fapte_2: Buget și Cereri
CREATE VIEW view_fact_budget_requests AS
SELECT 
    r.request_id,
    r.property_type,
    r.preferred_city,
    COUNT(r.request_id) AS total_requests,
    ROUND(AVG(r.max_budget), 2) AS avg_budget
FROM 
    property_requests r
GROUP BY 
    r.request_id, r.property_type, r.preferred_city;

-- 3. Tabele dimensionale

-- Tabela_dimensionala_1: Orașe
CREATE VIEW view_dim_city AS
SELECT 
    c.city,
    COUNT(r.request_id) AS total_requests,
    ROUND(AVG(r.max_budget), 2) AS avg_budget
FROM 
    customers c
JOIN 
    property_requests r ON c.id = r.customer_id
GROUP BY 
    c.city;

-- Tabela_dimensionala_2: Tipuri de Proprietăți
CREATE VIEW view_dim_property_type AS
SELECT 
    r.property_type,
    COUNT(r.request_id) AS total_requests,
    ROUND(AVG(r.max_budget), 2) AS avg_budget
FROM 
    property_requests r
GROUP BY 
    r.property_type;

-- 4. Tabele/view-uri cu agregări analitice (OLAP Analytical Views)


-- View_Analitic_OLAP_1: Buget și Cereri pe Orașe
CREATE VIEW view_analitic_olap_budget_per_city AS
SELECT 
    r.preferred_city,
    COUNT(r.request_id) AS total_requests,
    ROUND(AVG(r.max_budget), 2) AS avg_budget
FROM 
    property_requests r
GROUP BY 
    r.preferred_city;

-- View_Analitic_OLAP_2: Rating pe Tipuri de Proprietăți
CREATE VIEW view_analitic_olap_rating_per_property_type AS
SELECT 
    r.property_type,
    COUNT(f.id) AS feedback_count,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM 
    property_requests r
JOIN 
    request_feedback f ON r.request_id = f.request_id
GROUP BY 
    r.property_type;

-- 5. Funcții OLAP pentru CUBE

-- CUBE: combinații posibile oraș + rating
CREATE VIEW view_cube_city_rating AS
SELECT 
    c.city, 
    f.rating, 
    COUNT(*) AS total_feedback, 
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM 
    customers c
JOIN 
    property_requests r ON c.id = r.customer_id
JOIN 
    request_feedback f ON r.request_id = f.request_id
GROUP BY 
    CUBE(c.city, f.rating);



SELECT * FROM view_consolidare_customers_requests;
SELECT * FROM view_consolidare_requests_feedback;
SELECT * FROM view_fact_requests_feedback;
SELECT * FROM view_fact_budget_requests;
SELECT * FROM view_dim_city;
SELECT * FROM view_dim_property_type;
SELECT * FROM view_analitic_olap_budget_per_city;
SELECT * FROM view_analitic_olap_rating_per_property_type;
SELECT * FROM view_cube_city_rating;
SELECT * FROM view_rollup_city_property_type;
SELECT * FROM view_status_rating_per_city;


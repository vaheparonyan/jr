DELETE FROM sandbox.as_seo_cities;

SQL_BREAK;

DELETE FROM sandbox.as_potential_keywords;

SQL_BREAK;

INSERT INTO sandbox.as_seo_cities
SELECT location_url, 
	name, 
	OREPLACE(nameMedShort, ',', ''),
	ROW_NUMBER() OVER (ORDER BY population DESC)
FROM sandbox.seo_locations
WHERE location_type = 'city'
AND nameFull like '%United States';


SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT name,
	'/local/' || location_url,
	null,
	location_url,
	'<city>'
FROM sandbox.as_seo_cities;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT nameMedShort,
	'/local/' || location_url,
	null,
	location_url,
	'<cityMedShort>'
FROM sandbox.as_seo_cities
WHERE name <> nameMedShort;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT name || ' deals',
	'/local/' || location_url,
	null,
	location_url,
	'<city> deals'
FROM sandbox.as_seo_cities;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT nameMedShort || ' deals',
	'/local/' || location_url,
	null,
	location_url,
	'<cityMedShort> deals'
FROM sandbox.as_seo_cities
WHERE name <> nameMedShort;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.singular || ' ' || ci.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category singular> <city>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.singular || ' in ' || ci.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
    '<category singular> in <city>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ci.name || ' ' || ca.singular,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<city> <category singular>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.name || ' ' || ci.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category plural> <city>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.name || ' in ' || ci.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category plural> in <city>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ci.name || ' ' || ca.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<city> <category plural>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.singular || ' ' || ci.nameMedShort,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category singular> <cityMedShort>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.singular || ' in ' || ci.nameMedShort,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category singular> in <city>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ci.nameMedShort || ' ' || ca.singular,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<cityMedShort> <category singular>'
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca
WHERE ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.name || ' ' || ci.nameMedShort,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category plural> <cityMedShort>' 
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ca.name || ' in ' || ci.nameMedShort,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<category plural> in <cityMedShort>' 
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;

SQL_BREAK;

INSERT INTO sandbox.as_potential_keywords
SELECT ci.nameMedShort || ' ' || ca.name,
	'/local/' || ci.location_url || '/' || ca.url,
	ca.url,
	ci.location_url,
	'<cityMedShort> <category plural>' 
FROM sandbox.as_seo_cities ci, sandbox.seo_categories_with_names ca 
WHERE ca.name <> ca.singular 
AND ci.name <> ci.nameMedShort
AND ci.population_rank <= 500;
create table if not exists public.indeed_etl_jobs(
	id	SERIAL PRIMARY KEY,
	query VARCHAR(256),
	city VARCHAR(50),
	state VARCHAR(10),
	zip VARCHAR(15),
	radius INT, ---default is 25
	fromage INT,
	sort VARCHAR(20), ---date or relevance
	site_type VARCHAR (20), --- jobsite or employer default is all
	job_type VARCHAR (25),  --- Allowed values: "fulltime", "parttime", "contract", "internship", "temporary".
	country VARCHAR (20),
	channel VARCHAR (50),
	is_active boolean
);
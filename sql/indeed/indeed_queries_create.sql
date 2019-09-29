create table if not exists public.indeed_queries(
	job_key varchar(30),
	job_query varchar(50),
	primary key (job_key, job_query)
);

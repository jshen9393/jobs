create table if not exists public.indeed_jobs_duration(
	job_key varchar(30) primary key,
	job_date date,
	job_expired date,
	job_no_api date
);

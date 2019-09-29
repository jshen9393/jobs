insert into public.indeed_jobs_duration
select distinct
	job_key
	,job_date
	,to_date('1900-01-01','YYYY-MM-DD') job_expired
	,to_date('1900-01-01','YYYY-MM-DD') job_no_api
from public.indeed_jobs
where job_key not in (select job_key from public.indeed_jobs_duration)
;

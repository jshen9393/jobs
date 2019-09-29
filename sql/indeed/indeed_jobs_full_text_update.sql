insert into public.indeed_jobs_full_text

select distinct
	job_key
	, null job_text
from public.indeed_jobs
where job_key not in (select job_key from public.indeed_jobs_full_text)

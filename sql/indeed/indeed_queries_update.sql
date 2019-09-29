insert into public.indeed_queries
select distinct
	jobkey as job_key
	,jobquery as job_query
from public.indeed_stage_jobs
where jobkey||jobquery not in (select job_key||job_query from public.indeed_queries)
;
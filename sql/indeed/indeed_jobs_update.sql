insert into public.indeed_jobs
select distinct
	sj.jobkey as job_key
	, TO_DATE(split_part(sj.jobdate, ' ', 4) || '-' || mm.month_num || '-' || split_part(jobdate, ' ', 2), 'YYYY-MM-DD') as job_date
	, TO_TIMESTAMP(split_part(sj.jobdate, ' ', 5), 'HH24:MI:SS')::TIME as job_time
	, sj.jobtitle as job_title
	, sj.company
	, sj.city
	, sj.state
	, split_part(sj.jobdate, ' ', 3) as zip
	, sj.country
	, sj.latitude
	, sj.longitude
	, sj.jobsource as job_source
	, sj.url
	, sj.onmousedown as on_mouse_down
	, sj.sponsored
	, sj.expired
	, sj.indeedapply
	, sj.stations
from public.indeed_stage_jobs sj
	join public.indeed_mapping_month mm
	on split_part(sj.jobdate, ' ', 3) = mm.month_name
	join public.indeed_mapping_day dm
	on replace(split_part(sj.jobdate, ' ', 1),',','') = dm.day_name
	join (select jobkey, max(url||onmousedown) url_id
			from public.indeed_stage_jobs
			group by jobkey
			) ji ---different calls to api with different queries results in different urls
	on sj.url||sj.onmousedown = ji.url_id
where sj.jobkey not in (select job_key from public.indeed_jobs)
;
create table if not exists public.indeed_stage_jobs(
 	jobkey VARCHAR(30),
	jobquery VARCHAR(50),
	jobtitle VARCHAR(200),
	company VARCHAR(200),
	city VARCHAR(30),
	state VARCHAR(20),
	country VARCHAR(5),
	latitude FLOAT,
	longitude FLOAT,
	language VARCHAR(5),
	formattedlocation VARCHAR(30),
	jobsource VARCHAR(30),
	jobdate VARCHAR(40),
	url VARCHAR(300),
	onmousedown VARCHAR(30),
	sponsored BOOLEAN,
	expired BOOLEAN,
	indeedapply BOOLEAN,
	formattedlocationfull VARCHAR(50),
	formattedrelativetime VARCHAR(30),
	stations VARCHAR(30)
);
create table if not exists public.indeed_mapping_day(
day_num int,
day_name char(3)
);

insert into indeed_mapping_day (day_num, day_name) values(1,'Mon');
insert into indeed_mapping_day (day_num, day_name) values(2,'Tue');
insert into indeed_mapping_day (day_num, day_name) values(3,'Wed');
insert into indeed_mapping_day (day_num, day_name) values(4,'Thu');
insert into indeed_mapping_day (day_num, day_name) values(5,'Fri');
insert into indeed_mapping_day (day_num, day_name) values(6,'Sat');
insert into indeed_mapping_day (day_num, day_name) values(7,'Sun');

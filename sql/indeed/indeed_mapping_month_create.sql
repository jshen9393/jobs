create table if not exists public.indeed_mapping_month(
month_num char(2),
month_name char(3)
);

insert into indeed_mapping_month (month_num, month_name) values('01','Jan');
insert into indeed_mapping_month (month_num, month_name) values('02','Feb');
insert into indeed_mapping_month (month_num, month_name) values('03','Mar');
insert into indeed_mapping_month (month_num, month_name) values('04','Apr');
insert into indeed_mapping_month (month_num, month_name) values('05','May');
insert into indeed_mapping_month (month_num, month_name) values('06','Jun');
insert into indeed_mapping_month (month_num, month_name) values('07','Jul');
insert into indeed_mapping_month (month_num, month_name) values('08','Aug');
insert into indeed_mapping_month (month_num, month_name) values('09','Sep');
insert into indeed_mapping_month (month_num, month_name) values('10','Oct');
insert into indeed_mapping_month (month_num, month_name) values('11','Nov');
insert into indeed_mapping_month (month_num, month_name) values('12','Dec');
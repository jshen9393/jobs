from etl.sql.runner import runner

SQL_SCRIPTS =[
    'sql/indeed/indeed_etl_jobs_create.sql',
    'sql/indeed/indeed_jobs_create.sql',
    'sql/indeed/indeed_jobs_duration_create.sql',
    'sql/indeed/indeed_jobs_full_text_create.sql',
    'sql/indeed/indeed_mapping_day_create.sql',
    'sql/indeed/indeed_mapping_month_create.sql',
    'sql/indeed/indeed_queries_create.sql',
]


def main(*args, **kwargs):
    for script in SQL_SCRIPTS:
        runner.exec_sql_script(script,explicit_commit=True)
        print('Finished', script)


if __name__ == '__main__':
    main()

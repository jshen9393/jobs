
from etl.sql.runner import runner

SQL_SCRIPTS =[
    'sql/indeed/indeed_jobs_update.sql',
    'sql/indeed/indeed_queries_update.sql',
    'sql/indeed/indeed_jobs_full_text_update.sql',
    'sql/indeed/indeed_jobs_duration_update.sql',
]


def main():
    for script in SQL_SCRIPTS:
        runner.exec_sql_script(script,explicit_commit=True)
        print('Finished', script)


if __name__ == '__main__':
    main()

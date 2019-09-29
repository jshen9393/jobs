import psycopg2

from etl.indeed.indeed_extractor import IndeedDuration
from etl.common.db import get_postgres


def main(*args, **kwargs):

    extractor = IndeedDuration()

    with get_postgres() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            for job_key, expired, api_active in extractor.extract():
                print(job_key, expired, api_active)
                if expired:
                    cursor.execute(f"""
                        update public.indeed_jobs_duration
                        set job_expired = current_date
                        where job_key = '{job_key}'
                    """)
                elif not api_active:
                    cursor.execute(f"""
                        update public.indeed_jobs_duration
                        set job_no_api = current_date
                        where job_key = '{job_key}'
                    """)
            conn.commit()


if __name__ == "__main__":
    main()
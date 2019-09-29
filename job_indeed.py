from etl.utils.common import get_script_name

import indeed_etl
import indeed_sql_update
import indeed_duration_update

SCRIPTS =[
    indeed_etl,
    indeed_sql_update,
    indeed_duration_update,
]

def main():
    parent_script_name = get_script_name()

    for script in SCRIPTS:
        print(script.__name__, parent_script_name)
        script.main(script_name=script.__name__, parent_script_name=parent_script_name)


if __name__ == '__main__':
    main()

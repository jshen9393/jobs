import sqlite3

def create_tables():

    conn = sqlite3.connect("jobs.db")
    c = conn.cursor()

    c.execute('''
    CREATE TABLE IF NOT EXISTS jobs
    (jobtitle text, company text, city text, state text, source text, date text, 
    url text, jobkey text primary key, formattedLocationFull text)
    ''')

    c.close()


def insert_into(cursor,**kwargs):

    kwargs['url'] = kwargs['url'].split('&qd')[0]

    try:
        cursor.execute(f'''
        INSERT INTO jobs(jobtitle, company, city, state, source, date, url, jobkey, formattedLocationFull)
        VALUES("{kwargs['jobtitle']}", "{kwargs['company']}", "{kwargs['city']}", "{kwargs['state']}",
        "{kwargs['source']}", "{kwargs['date']}", "{kwargs['url']}", "{kwargs['jobkey']}", "{kwargs['formattedLocationFull']}")
        '''
        )
    except:
        print(f"insert failed for jobkey {kwargs['jobkey']}")



if __name__ == "__main__":
    create_tables()
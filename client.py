import requests
import psycopg2


def con_db():
    db = None
    try:
        db = psycopg2.connect(
        host="192.168.199.208",
        database="postgres",
        user="postgres",
        password="sa"
        )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if db is not None:
            cur = db.cursor()
        
            # execute a statement
            print('Connected to the PostgreSQL database...')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            
    return db


def sync_fname():
    print(">>>>FNAME Start<<<<")
    api = f"{API}/fname"
    resp = requests.get(api)
    data = resp.json()
    db = con_db()
    cur = db.cursor()
    sql = "DELETE FROM smartq_fname;"
    cur.execute(sql)
    print("Clear Table Success!")
    
    for mydict in data:
        columns = ', '.join(str(x).replace('/', '_') for x in mydict.keys())
        columns = columns + ",d_update"

        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
        values = values + ",CURRENT_TIMESTAMP(0)"

        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('smartq_fname', columns, values)
        cur.execute(sql)
        print(sql)

    db.commit()
    db.close()
    print('FNAME OK.....')


def sync_lname():
    print(">>>>LNAME Start<<<<")
    api = f"{API}/lname"
    resp = requests.get(api)
    data = resp.json()
    db = con_db()
    cur = db.cursor()
    sql = "DELETE FROM smartq_lname;"
    cur.execute(sql)
    print("Clear Table Success!")
    
    for mydict in data:
        columns = ', '.join(str(x).replace('/', '_')  for x in mydict.keys())
        columns = columns + ",d_update"

        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
        values = values + ",CURRENT_TIMESTAMP(0)"

        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('smartq_lname', columns, values)
        cur.execute(sql)
        print(sql)

    db.commit()
    db.close()
    print('LNAME OK...')


if __name__ == '__main__':
    API = "http://www.smartqplk.com:5000"
    sync_fname()
    print('.......')
    sync_lname()

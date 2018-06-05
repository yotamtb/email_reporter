import mysql.connector

import config


def connect_to_db():
    conn = mysql.connector.connect(user=config.DB_USER,
                                   password=config.DB_PASSWORD,
                                   host=config.DB_HOST,
                                   database=config.DB_USER)
    print 'Connected to database successfully'
    return conn


def upsert_report(report_data):
    try:
        print 'updating report in db'
        conn = connect_to_db()
        cursor = conn.cursor()
        upsert_query = '''
                INSERT INTO {tname} ({cols})
                    VALUES (%(id)s, %(date)s, %(from)s, %(to)s, %(subject)s, %(s3_link)s)
                ON DUPLICATE KEY UPDATE
                {update_cols}'''.format(tname=config.REPORT_TABLE_NAME,
                                        cols=','.join(config.COLUMNS),
                                        update_cols=','.join([c+'=values('+c+')' for c in config.COLUMNS]))

        for entity in report_data:
            cursor.execute(upsert_query, entity)

        # Make sure data is committed to the database
        conn.commit()
        print 'report updated successfully'
    except Exception as e:
        print str(e)
        raise
    finally:
        cursor.close()
        conn.close()


#!/usr/bin/python3

import psycopg2
from psycopg2 import sql
import re
import string
import sys

_PUNCTUATION = frozenset(string.punctuation)

def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
    return "" if i > j else token[i:(j+1)]

def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "''")
            rewritten_query.append(cleaned_token)
    return rewritten_query



def search(query, query_type, n_page):
    
    rewritten_query = _get_tokens(query)

    """TODO
    1. Connect to the Postgres database.
    2. Graciously handle any errors that may occur (look into try/except/finally).
    3. Close any database connections when you're done.
    4. Write queries so that they are not vulnerable to SQL injections.
    5. The parameters passed to the search function may need to be changed for 1B. 
    """

    try:
        conn = psycopg2.connect(dbname="searchengine", user="cs143", password="cs143")
        cursor = conn.cursor()
        cursor.execute('DROP MATERIALIZED VIEW IF EXISTS furuya;')
        conn.commit()
        if query_type.lower() == 'or':
            query = sql.SQL('CREATE MATERIALIZED VIEW furuya AS\
                             SELECT S.song_name, A.artist_name, S.page_link, L.song_id, SUM(L.score) as score\
                             FROM tfidf L\
                             LEFT JOIN song S ON L.song_id=S.song_id\
                             LEFT JOIN artist A ON S.artist_id=A.artist_id'
                             + ' WHERE token IN %s' 
                             + 'GROUP BY L.song_id, A.artist_name, S.song_name, S.page_link\
                             ORDER BY score DESC;')
            cursor.execute(query, (tuple(rewritten_query),))
            cursor.execute('SELECT COUNT(*) FROM furuya')
            n_row = cursor.fetchone()[0]
            #print ("n_row:", n_row)

            #cursor.execute('REFRESH MATERIALIZED VIEW furuya',)
            cursor.execute('SELECT * FROM furuya ORDER BY score DESC LIMIT 20;')
            rows = cursor.fetchall()
            #print(rows)
            conn.commit()

        else:
            query = sql.SQL('CREATE MATERIALIZED VIEW furuya AS\
                            SELECT S.song_name, A.artist_name, S.page_link, L.song_id, SUM(L.score) AS score\
                            FROM tfidf L\
                            LEFT JOIN song S ON L.song_id=S.song_id\
                            LEFT JOIN artist A ON S.artist_id=A.artist_id\
                            INNER JOIN (\
                            SELECT song_id\
                            FROM (\
                            SELECT song_id, token\
                            FROM tfidf\
                            WHERE token IN %s) rei\
                            GROUP BY song_id\
                            HAVING COUNT(1) = %s\
                            ) R ON L.song_id = R.song_id\
                            WHERE token IN %s\
                            GROUP BY L.song_id, A.artist_name, S.song_name, S.page_link\
                            ORDER BY score DESC;' 
                            )
            #for i in range (0, len(rewritten_query)):
                #print(rewritten_query[i])
            #print (len(rewritten_query))

            cursor.execute(query, (tuple(rewritten_query),str(len(tuple(rewritten_query))),tuple(rewritten_query),))
            cursor.execute('SELECT COUNT(*) FROM furuya')
            n_row = cursor.fetchone()[0]
            cursor.execute('SELECT * FROM furuya ORDER BY score DESC LIMIT 20;')
            rows = cursor.fetchall()
            conn.commit()
            
    except Exception as e:
        print("error when fetching: ", e)
    finally:
        if conn is not None:
            conn.close()
        if cursor is not None:
            cursor.close()
    return n_row, rows

def pagination(n_page):
    try:
        conn = psycopg2.connect(dbname="searchengine", user="cs143", password="cs143")
        cursor = conn.cursor()
        if n_page == 0:
            n_page=1
        query = 'SELECT * FROM furuya LIMIT 20 OFFSET %s'
        cursor.execute(query, [str((n_page-1)*20)])
        rows = cursor.fetchall()
        cursor.execute('SELECT COUNT(*) FROM furuya')
        n_row = cursor.fetchone()[0]
        conn.commit()
        return n_page, n_row, rows
    except Exception as e:
        print("error when fetching", e)
    finally:
        if conn is not None:
            conn.close()
        if cursor is not None:
            cursor.close()






if __name__ == "__main__":
    if len(sys.argv) > 2:
        result = search(' '.join(sys.argv[2:]), sys.argv[1].lower(), '')
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")


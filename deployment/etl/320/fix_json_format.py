import ast
import json
import pymysql as MySQLdb
import sys


def get_connection():
    connection = MySQLdb.connect(
        host="jobmon-prod-1.db.ihme.washington.edu",
        user="service_user",
        password="",
        database="docker")
    return connection

def generate_sql(start_id):
    current_id = start_id
    connection = get_connection()

    cur = connection.cursor()
    cur.execute("select max(id) from task_resources;")
    max_id = cur.fetchone()[0]

    total = 0
    while current_id < max_id:
        print(f"current_id: {current_id}")
        cur.execute(f"SELECT id, requested_resources FROM task_resources WHERE id >= {current_id} limit 10000;")
        rows = cur.fetchall()
        statements = []
        if len(rows) > 0:
            for row in rows:
                id = row[0]
                rr = row[1]
                try:
                    json.loads(rr)
                except:
                    rr = json.dumps(ast.literal_eval(rr))
                    rr = rr.replace("'", "''")
                    sql_statement = f"UPDATE task_resources SET requested_resources='{rr}' WHERE id={id};"
                    statements.append(sql_statement)
                    #sql_file.write(sql_statement)

        # update database
        for statement in statements:
            cur.execute(statement)
        connection.commit()
        print(f"Updated {len(statements)}")
        total += len(statements)
        current_id += 10_000
    connection.close()

    print(f"max_id: {max_id}")
    print(f"total lines updated: {total}")

if __name__ == '__main__':
    start_id = 0
    if len(sys.argv) > 1:
        start_id = int(sys.argv[1])
    generate_sql(start_id)
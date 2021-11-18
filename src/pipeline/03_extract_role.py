import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_role_data(cur, con):
  sql = """
    INSERT INTO dim_role(name)
    SELECT
      DISTINCT 
      (CASE 
        WHEN employee_role LIKE '%Mgr%' OR employee_role LIKE '%Supv%' THEN 'Manager' 
        ELSE 'Employee'
      END
      ) AS name
    FROM raw_employees;
  """
  cur.execute(sql)
  con.commit()

def main():
  con = connect()
  cur = con.cursor()

  extract_role_data(cur, con)

  cur.close()
  con.close()

if __name__ == "__main__":
  main()

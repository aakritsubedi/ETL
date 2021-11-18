import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_status_data(cur, con):
  sql = """
    INSERT INTO dim_status(name)
    SELECT
      DISTINCT
      (CASE 
        WHEN terminated_date = '01-01-1700' THEN 'Active' 
        ELSE 'Terminated'
      END
      ) AS name
    FROM raw_employees;
  """
  cur.execute(sql)
  con.commit()

def main():
  con = connect()
  cur = con.cursor()

  extract_status_data(cur, con)

  cur.close()
  con.close()

if __name__ == "__main__":
  main()

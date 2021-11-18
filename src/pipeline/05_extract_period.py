import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_period_data(cur, con):
  sql = """
    INSERT INTO dim_period(start_date, end_date)
    SELECT
      CAST(hire_date AS DATE) AS start_date,
      CAST((CASE 
        WHEN terminated_date = '01-01-1700' THEN NULL 
        ELSE terminated_date
      END)
      AS DATE) AS end_date
    FROM raw_employees;
  """
  cur.execute(sql)
  con.commit()

def main():
  con = connect()
  cur = con.cursor()

  extract_period_data(cur, con)

  cur.close()
  con.close()

if __name__ == "__main__":
  main()

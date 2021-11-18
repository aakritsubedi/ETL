import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_department_data(cur, con):
  sql = """
    INSERT INTO dim_department(department_id, name)
    SELECT 
      DISTINCT 
      CAST(department_id AS INT),
      department_name AS name
    FROM raw_employees;
  """
  cur.execute(sql)
  con.commit()

def main():
  con = connect()
  cur = con.cursor()

  extract_department_data(cur, con)

  cur.close()
  con.close()

if __name__ == "__main__":
  main()

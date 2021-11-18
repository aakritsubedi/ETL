import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_shift_type_data(cur, con):
  sql = """
    INSERT INTO dim_shift(name) 
    SELECT 
      DISTINCT 
      (CASE
        WHEN
          MIN(TO_TIMESTAMP (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '05:00:00.000000' AND
          MIN(TO_TIMESTAMP (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '12:00:00' then 'Morning'
        WHEN MIN(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '12:00:00.000000' AND
        MIN(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '17:00:00' then 'Afternoon'
        END
      ) AS shift_type
      FROM raw_timesheets r GROUP BY punch_apply_date, paycode having paycode= 'WRK'
  """
  
  cur.execute(sql)
  con.commit()


def main():
  con = connect()
  cur = con.cursor()

  extract_shift_type_data(cur, con)

  cur.close()
  con.close()



if __name__ == "__main__":
  main()

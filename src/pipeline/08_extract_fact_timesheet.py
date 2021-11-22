import psycopg2

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def extract_fact_timesheet_data(cur, con):
  sql = """
    WITH t_hours_worked AS (
      SELECT 
        employee_id, 
        SUM(CAST(hours_worked AS FLOAT)) AS hours_worked,
        (CASE
        WHEN
          MIN(TO_TIMESTAMP (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '05:00:00.000000' AND
          MIN(TO_TIMESTAMP (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '12:00:00' THEN (SELECT id FROM dim_shift WHERE name = 'Morning')
        WHEN MIN(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) >= '12:00:00.000000' AND
          MIN(to_timestamp (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) < '17:00:00' 
          THEN (SELECT id FROM dim_shift WHERE name = 'Afternoon')
        END
      ) AS shift_type_id,
      MIN(TO_TIMESTAMP (punch_in_time, 'YYYY.MM.DD HH24:MI:SS')::time) AS punch_in_time,
      MAX(TO_TIMESTAMP (punch_out_time, 'YYYY.MM.DD HH24:MI:SS')::time) AS punch_out_time
      FROM raw_timesheets 
      WHERE paycode = 'WRK'
      GROUP BY employee_id, punch_apply_date
    ),
    t_attendance AS (
      SELECT
      employee_id,
      (CASE 
        WHEN paycode = 'ABSENT' THEN FALSE ELSE TRUE
      END
      ) AS attendance
      FROM raw_timesheets
    ),
    t_break AS (
      SELECT
      employee_id,
      (CASE 
        WHEN paycode = 'BREAK' THEN TRUE ELSE FALSE
      END) AS has_taken_break,
      (CASE 
        WHEN paycode = 'BREAK' 
        THEN SUM(CAST(hours_worked AS FLOAT)) 
        ELSE 0
      END) AS break_hour
      FROM raw_timesheets
      GROUP BY employee_id, paycode, punch_apply_date
    ),
    t_charge AS (
      SELECT
      employee_id,
      (CASE 
        WHEN paycode = 'CHARGE' THEN TRUE ELSE FALSE
      END) AS was_charge,
      (CASE 
        WHEN paycode = 'CHARGE' 
        THEN SUM(CAST(hours_worked AS FLOAT)) 
        ELSE 0
      END) AS charge_hour
      FROM raw_timesheets
      GROUP BY employee_id, paycode, punch_apply_date
    ),
    t_on_call AS (
      SELECT
      employee_id,
      (CASE 
        WHEN paycode = 'ON_CALL' THEN TRUE ELSE FALSE
      END) AS was_on_call,
      (CASE 
        WHEN paycode = 'ON_CALL' 
        THEN SUM(CAST(hours_worked AS FLOAT)) 
        ELSE 0
      END) AS on_call_hour
      FROM raw_timesheets
      GROUP BY employee_id, paycode, punch_apply_date
    )
    INSERT INTO fact_timesheet(employee_id, department_id, work_date, hours_worked, shift_type_id, punch_in_time, punch_out_time, attendance, work_code, has_taken_break, break_hour, was_charge, charge_hour, was_on_call, on_call_hour, is_weekend, num_teammates_absent)
    SELECT 
      fe.employee_id,
      rt.cost_center AS department_id,
      CAST(rt.punch_apply_date AS DATE) AS work_date,
      hw.hours_worked as hours_worked,
      hw.shift_type_id,
      hw.punch_in_time,
      hw.punch_out_time,
      -- time_period_id,
      ta.attendance,
      paycode AS work_code,
      tb.has_taken_break,
      tb.break_hour,
      tc.was_charge,
      tc.charge_hour,
      toc.was_on_call,
      toc.on_call_hour,
      NULL AS is_weekend,
      (SELECT COUNT(*) num_teammates_absent FROM raw_timesheets r WHERE r.cost_center = rt.cost_center AND r.punch_apply_date = rt.punch_apply_date)
    FROM raw_timesheets rt
    JOIN fact_employee fe 
    ON rt.employee_id = fe.client_employee_id
    JOIN t_hours_worked hw
    ON rt.employee_id = hw.employee_id
    JOIN t_attendance ta
    ON rt.employee_id = ta.employee_id
    JOIN t_break tb
    ON rt.employee_id = tb.employee_id
    JOIN t_charge tc
    ON rt.employee_id = tc.employee_id
    JOIN t_on_call toc
    ON rt.employee_id = toc.employee_id
  """
  
  cur.execute(sql)
  con.commit()


def main():
  con = connect()
  cur = con.cursor()

  extract_fact_timesheet_data(cur, con)

  cur.close()
  con.close()



if __name__ == "__main__":
  main()


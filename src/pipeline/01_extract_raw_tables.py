import json 
import psycopg2
import xmltodict

def connect():
  return psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="etl"
  )

def readFromCSV(filepath):
  data = []
  i=0
  with open(filepath, 'r') as file:
    for line in file:
      # Skipping headers
      if i == 0:
        i += 1
        continue

      data.append(line)

  return data

def readFromJSON(filepath):
  with open(filepath, 'r') as file:
    data = file.read()

    return data


def readFromXML(filepath):
  data = []
  with open(filepath, 'r') as file:
    data = xmltodict.parse(file.read())
    data = json.dumps(data)
    data = json.loads(data)
    data = data['EmployeeList']['Employee']
    
  return data

def readFromFile(format, filepath):
  if(format == 'csv'):
    return readFromCSV(filepath)
  elif (format == 'json'):
    return readFromJSON(filepath)
  elif (format == 'xml'):
    return readFromXML(filepath)
  else:
    print('Invalid Format')

def extract_employee_data(format, filepath, cur, con):
  employee_sql="""
    INSERT INTO raw_employees (employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  """

  # Inserting Into raw employees table
  employees = readFromFile(format, filepath)
  for employee in employees:
    if format == 'csv':
      employee = employee.strip().split(',')
    else:
      employee = list(employee.values())
    cur.execute(employee_sql, employee)
    con.commit()

def extract_timesheet_data(format, filepath, cur, con):
  timesheets_sql="""
    INSERT INTO raw_timesheets(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode) VALUES(%s, %s, %s, %s, %s, %s, %s)
  """
  
  # Inserting Into raw timesheets table
  timesheets = readFromFile(format,  filepath)

  for timesheet in timesheets:
    if format == 'csv':
      timesheet = timesheet.strip().split(',')
    else:
      timesheet = list(timesheet.values())
    
    cur.execute(timesheets_sql, timesheet)
    con.commit()

def truncate_table(table_name, cur, con):
  sql = "TRUNCATE TABLE " + table_name
  cur.execute(sql)
  con.commit()

def archive_employee_data(filename, scon, scur, dcon, dcur):
  emp_sql = "SELECT * FROM raw_employees"
  scur.execute(emp_sql)
  employees = scur.fetchall()

  employee_sql="""
    INSERT INTO archive_employees (employee_id,first_name,last_name,department_id,department_name,manager_employee_id,employee_role,salary,hire_date,terminated_date,terminated_reason,dob,fte,location, filename) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

  for employee in employees:
    row = list(employee)
    row.append(filename)
    row = tuple(row)

    dcur.execute(employee_sql, row)
    dcon.commit()
    

def archive_timesheet_data(filename, scon, scur, dcon, dcur):
  timesheet_sql = "SELECT * FROM raw_timesheets"
  scur.execute(timesheet_sql)
  timesheets = scur.fetchall()

  timesheets_sql="""
    INSERT INTO archive_timesheets(employee_id,cost_center,punch_in_time,punch_out_time,punch_apply_date,hours_worked,paycode, filename) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
  """

  for timesheet in timesheets:
    row = list(timesheet)
    row.append(filename)
    row = tuple(row)

    dcur.execute(timesheets_sql, row)
    dcon.commit()

def main():
  con = connect()
  cur = con.cursor()

  # Truncate Tables
  truncate_table('raw_employees', cur, con)
  truncate_table('raw_timesheets', cur, con)

  # Insert into raw tables
  extract_employee_data('xml', 'data/employee_2021_08_01.xml', cur, con)
  extract_timesheet_data('csv', 'data/timesheet_2021_05_23.csv', cur, con)
  extract_timesheet_data('csv', 'data/timesheet_2021_06_23.csv', cur, con)
  extract_timesheet_data('csv', 'data/timesheet_2021_07_24.csv', cur, con)

  # Archive data
  dest_con = connect()
  dest_cur = dest_con.cursor()

  # Archive data
  archive_employee_data('employee_2021_08_01.xml', con, cur, dest_con, dest_cur)
  archive_timesheet_data('timesheet_2021_05_23.csv',  con, cur, dest_con, dest_cur)

  cur.close()
  con.close()

if __name__ == "__main__":
  main()

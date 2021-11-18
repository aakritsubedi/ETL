CREATE TABLE fact_employee(
  employee_id SERIAL PRIMARY KEY,
  client_employee_id VARCHAR(255),
  department_id VARCHAR(255),
  manager_id VARCHAR(255),
  role_id INT,
  salary FLOAT,
  active_status_id INT,
  weekly_hours FLOAT,
  FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
  FOREIGN KEY(role_id) REFERENCES dim_role(role_id),
  FOREIGN KEY(active_status_id) REFERENCES dim_status(status_id)
);

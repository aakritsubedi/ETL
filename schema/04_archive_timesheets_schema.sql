-- Creating archive_timesheets table
CREATE TABLE archive_timesheets(
  employee_id VARCHAR(255),
  cost_center VARCHAR(255),
  punch_in_time VARCHAR(255),
  punch_out_time VARCHAR(255),
  punch_apply_date VARCHAR(255),
  hours_worked VARCHAR(255),
  paycode VARCHAR(255),
  filename VARCHAR(255)
);

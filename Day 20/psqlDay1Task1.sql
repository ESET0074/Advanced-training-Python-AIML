/* PART 1: CREATE TABLES + DATA TYPES + CONSTRAINTS
   Task 1: Create tables with appropriate data types
   Task 2: Use constraints: NOT NULL, UNIQUE, PRIMARY KEY
   Task 3: Define default values and check constraints
   Task 4: Understand and use SERIAL and UUID types
   Task 5: Alter tables to add/drop columns and constraints */

CREATE TABLE aimlemployee (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL
);

INSERT INTO aimlemployee(id, name, city)
VALUES 
(1,'Aniket','Siliguri'),
(2,'Gaurav','MP'),
(3,'Milan','Rajasthan'),
(4,'Uday','Gujarat'),
(5,'Sourav','Bihar'),
(6,'Abishek','MP'),
(7,'Subham','Pune'),
(8,'Reetu Raj','India'),
(9,'Gopal','Gujarat');

ALTER TABLE aimlemployee ADD COLUMN salary INT DEFAULT 50000;

UPDATE aimlemployee
SET salary = CASE id
    WHEN 1 THEN 100000
    WHEN 2 THEN 80000
    WHEN 3 THEN 85000
    WHEN 4 THEN 45000
    WHEN 5 THEN 60000
    WHEN 6 THEN 90000
    WHEN 7 THEN 59000
    WHEN 8 THEN 79000
    WHEN 9 THEN 25000
END;

ALTER TABLE aimlemployee
ADD CONSTRAINT uq_aimlemployee UNIQUE (id);

ALTER TABLE aimlemployee
ADD COLUMN serial_id SERIAL;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE aimlemployee
ADD COLUMN uuid_col UUID DEFAULT uuid_generate_v4();


/* PART 2: JOINS AND ADDITIONAL TABLE FOR JOIN OPERATIONS
   Task 1: Implement INNER JOIN, LEFT JOIN, RIGHT JOIN */

CREATE TABLE employedetails (
    id INT PRIMARY KEY,
    department VARCHAR(50),
    age INT CHECK (age > 0)
);

INSERT INTO employedetails (id, department, age) VALUES
(1, 'HR', 25),
(2, 'Finance', 28),
(3, 'IT', 30),
(4, 'Marketing', 27),
(5, 'Sales', 26),
(6, 'IT', 29),
(7, 'HR', 31),
(8, 'Finance', 24),
(9, 'Sales', 33);

/* INNER JOIN */
SELECT * 
FROM aimlemployee a
INNER JOIN employedetails e ON a.id = e.id;

/* LEFT JOIN */
SELECT *
FROM aimlemployee a
LEFT JOIN employedetails e ON a.id = e.id;

/* RIGHT JOIN */
SELECT *
FROM aimlemployee a
RIGHT JOIN employedetails e ON a.id = e.id;


/* PART 3: SUBQUERIES / EXISTS / IN
   Task 2: Write simple correlated and non-correlated subqueries
   Task 3: Use EXISTS and IN predicates */

CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(50)
);

CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    emp_name VARCHAR(50),
    dept_id INT,
    salary INT,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

INSERT INTO departments (dept_name) VALUES
('HR'), ('Finance'), ('IT'), ('Marketing');

INSERT INTO employees (emp_name, dept_id, salary) VALUES
('Amit', 1, 50000),
('Rahul', 1, 60000),
('Sneha', 2, 70000),
('Ravi', 2, 45000),
('Priya', 3, 90000),
('Ankit', 3, 85000),
('Meena', 4, 40000),
('Vikram', 4, 55000);

/* Non-correlated subquery */
SELECT emp_name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

/* Correlated subquery */
SELECT emp_name, salary, dept_id
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE dept_id = e.dept_id
);

/* EXISTS predicate */
SELECT dept_name
FROM departments d
WHERE EXISTS (
    SELECT 1
    FROM employees e
    WHERE e.dept_id = d.dept_id
    AND e.salary > 80000
);

/* IN predicate */
SELECT emp_name, salary
FROM employees
WHERE salary IN (50000, 60000, 70000);


/* PART 4: UNION AND UNION ALL
   Task 4: UNION and UNION ALL */

SELECT id, salary FROM aimlemployee WHERE id < 9
UNION
SELECT emp_id AS id, salary FROM employees LIMIT 8;

SELECT id, salary FROM aimlemployee WHERE id < 9
UNION ALL
SELECT emp_id AS id, salary FROM employees LIMIT 8;

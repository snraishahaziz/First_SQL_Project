-- Creating a database
CREATE DATABASE Hospital_Management;
USE Hospital_Management;

CREATE TABLE staff (
	staff_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(100),
    staff_role VARCHAR(20),
    service VARCHAR(20)
);

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Hospital Management/staff.csv'
INTO TABLE staff
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Number of staff in each roles
SELECT
	staff_role,
    COUNT(*) AS value_count
FROM staff
GROUP BY staff_role;

-- Number of staff in each service departments
SELECT
	service,
    COUNT(*) AS value_count
FROM staff
GROUP BY service;

CREATE TABLE schedule (
	week_num INT,
	staff_id VARCHAR(50),
    full_name VARCHAR(100),
    staff_role VARCHAR(20),
    service VARCHAR(20),
    presence boolean,
    PRIMARY KEY (week_num, staff_id)
);

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Hospital Management/staff_schedule.csv'
INTO TABLE schedule
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Staff absence and presence count sorted by highest presence to lowest
SELECT 
	full_name, staff_id, staff_role, service,
	COUNT(CASE WHEN presence=1 THEN 1 END) AS true_count,
	COUNT(CASE WHEN presence=0 THEN 1 END) AS false_count
FROM schedule
GROUP BY full_name, staff_id, staff_role, service
ORDER BY true_count DESC;

-- Number of staff working each week of each department
WITH ranked AS (
	SELECT
		week_num, service,
		COUNT(staff_id) OVER (PARTITION BY week_num, service) AS number_of_staff,
		ROW_NUMBER() OVER (PARTITION BY week_num, service) AS rn
	FROM schedule
)
SELECT
	week_num, service, number_of_staff
FROM ranked
WHERE rn = 1;

-- Number of staff presence by departments
WITH ranked AS (
	SELECT
		*,
        ROW_NUMBER() OVER (PARTITION BY week_num, service ORDER BY week_num) AS rn,
		COUNT(CASE WHEN presence=1 THEN 1 END) OVER (PARTITION BY week_num, service) AS true_count,
		COUNT(CASE WHEN presence=0 THEN 1 END) OVER (PARTITION BY week_num, service) AS false_count
        FROM schedule
)
SELECT
	week_num, service, true_count, false_count
FROM ranked
WHERE rn = 1 AND true_count != 0
ORDER BY week_num;

CREATE TABLE service (
	week_num INT,
    month_num INT,
    service VARCHAR(50),
    available_beds INT,
    request INT,
    admitted INT,
    refused INT,
    patient_satisfaction INT,
    staff_morale INT,
    event_occured VARCHAR(50),
    PRIMARY KEY (week_num, month_num, service)
    );

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Hospital Management/services_weekly.csv'
INTO TABLE service
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Number of event occurrences each week
SELECT
		week_num, month_num, event_occured,
		COUNT(event_occured) AS occurrence
	FROM service
	GROUP BY week_num, month_num, event_occured
    ORDER BY week_num ASC, occurrence DESC;

-- Event with highest occurrences each week
WITH ranked AS (
	SELECT
		week_num, month_num, event_occured,
        COUNT(event_occured) AS occurrence,
		DENSE_RANK() OVER (
			PARTITION BY week_num
			ORDER BY COUNT(event_occured) DESC) as ranking
	FROM service
    GROUP BY week_num, month_num, event_occured)
SELECT
	week_num, month_num, event_occured, occurrence
FROM ranked
WHERE 
	ranking = 1
GROUP BY week_num, month_num, event_occured, occurrence
ORDER BY week_num ASC, occurrence DESC;

-- Number of admission each week    
SELECT
	week_num, month_num,
    SUM(admitted) AS number_admitted
FROM service
GROUP BY week_num, month_num;

-- Average number of admission each month sorted from highest to lowest average
SELECT
	month_num,
    AVG(admitted) AS average_admitted
FROM service
GROUP BY month_num
ORDER BY average_admitted DESC;

-- Does patient refusal of admission affect patient satisfaction
WITH base AS(
	SELECT
		*,
		(refused * 100) / request AS refused_percentage
	FROM service
),
classification AS(
	SELECT
		*,
		CASE
			WHEN patient_satisfaction >= 50 AND refused_percentage <= 50
				THEN 'Affected'
			WHEN patient_satisfaction < 50 AND refused_percentage > 50
				THEN 'Affected'
			ELSE 'Not affected'
		END AS Possibility_of_Effect    
	FROM base
),
summary AS (
	SELECT
		SUM(Possibility_of_Effect = 'Affected') AS Affected,
		SUM(Possibility_of_Effect = 'Not affected') AS Not_affected
	FROM classification
)
SELECT
	month_num, week_num, service, refused_percentage,
    patient_satisfaction, Possibility_of_Effect
FROM classification
UNION ALL
SELECT
	'Totals', '', '', '', Affected, Not_affected
FROM summary;

-- Which week/month has the highest/lowest staff_morale and does event occurrence affect
-- staff's morale
WITH base AS (
	SELECT
		*,
		CASE
			WHEN event_occured != 'none' THEN 1
			ELSE 0
		END AS 'Event'
	FROM service
),
ranked AS (
	SELECT
		*,
        DENSE_RANK() OVER (PARTITION BY week_num ORDER BY month_num, staff_morale DESC) AS rn
    FROM base
),
summary AS (
	SELECT
		*,
		CASE
			WHEN Event = 1 and staff_morale < 50 THEN 'Affected'
            WHEN Event = 0 and staff_morale >= 50 THEN 'Affected'
            ELSE 'Not affected'
		END AS effects
	FROM ranked
),
total_summary AS (
	SELECT
		SUM(effects = 'Affected') AS total_affected,
        SUM(effects = 'Not affected') AS total_not_affected
	FROM summary
)
SELECT
	week_num, month_num, service, Event, staff_morale
FROM summary
WHERE rn = 1
UNION ALL
SELECT
	'total', '', '', total_not_affected, total_affected
FROM total_summary
ORDER BY staff_morale DESC;

-- Does event occurrence affect patient admission
WITH base AS (
	SELECT
		week_num, month_num, event_occured, available_beds, admitted, service
	FROM service
),
averaging AS (
SELECT
	week_num, month_num, event_occured, available_beds, admitted, service,
    AVG(admitted) OVER (PARTITION BY month_num, service) AS average
FROM base
),
class AS (
	SELECT
        week_num, month_num, event_occured, available_beds, admitted, average, service,
		CASE
			WHEN event_occured != 'none' AND admitted > average THEN 'Affected'
            ELSE 'Not affected'
		END AS Effect
	FROM averaging
    GROUP BY week_num, month_num, event_occured, available_beds, admitted, average, service
)
SELECT 
	SUM(Effect = 'Affected') AS total_affected, SUM(Effect = 'Not affected') AS total_not_affected
FROM class;


CREATE TABLE patients (
	patient_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
    arrival DATE,
    departure DATE,
    service VARCHAR(50),
    satisfaction INT
    );
    
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Hospital Management/patients.csv'
INTO TABLE patients
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Total patients admitted per month and average patients admitted per week
WITH base AS (
	SELECT
		patient_id, full_name, age, arrival, departure, service, satisfaction,
        EXTRACT(YEAR FROM arrival) AS arrival_year,
        EXTRACT(MONTH FROM arrival) AS arrival_month,
        WEEK(arrival,4) AS arrival_week
	FROM patients
    ORDER BY arrival_year, arrival_month, arrival_week
),
ranked AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY arrival_week ORDER BY arrival_week) AS rn
	FROM base
),
calculate AS (
	SELECT
		*,
		COUNT(patient_id) OVER (PARTITION BY arrival_month) AS patient_arrived_month,
        COUNT(patient_id) OVER (PARTITION BY arrival_week) AS patient_arrived_week
	FROM ranked
)
SELECT
	arrival_month, arrival_week, patient_arrived_month, patient_arrived_week,
    AVG(patient_arrived_week) OVER (PARTITION BY arrival_month) AS average_patient_week
FROM calculate
WHERE rn = 1;

-- Average age of admitted patient per department per month
WITH base AS (
	SELECT
		age, service,
        EXTRACT(YEAR FROM arrival) AS arrival_year,
        EXTRACT(MONTH FROM arrival) AS arrival_month,
        WEEK(arrival,4) AS arrival_week
	FROM patients
    ORDER BY arrival_year, arrival_month, arrival_week
),
ranked AS (
	SELECT
		age, arrival_month, service,
		AVG(age) OVER (PARTITION BY arrival_month, service) AS average_age,
		ROW_NUMBER() OVER (PARTITION BY arrival_month, service) AS rn
	FROM base
)
SELECT
	arrival_month, service, average_age
FROM ranked
WHERE rn = 1
ORDER BY arrival_month, service;

-- Youngest and oldest patient of each services per month
WITH base AS (
	SELECT
		patient_id, full_name, age, service,
        EXTRACT(YEAR FROM arrival) AS arrival_year,
        EXTRACT(MONTH FROM arrival) AS arrival_month
	FROM patients
    ORDER BY arrival_year, arrival_month
),
ranked AS (
	SELECT
		arrival_month, patient_id, full_name, service,
		MIN(age) OVER (PARTITION BY arrival_month, service) AS youngest_patient,
		MAX(age) OVER (PARTITION BY arrival_month, service) AS oldest_patient,
        ROW_NUMBER() OVER (PARTITION BY arrival_month, service) AS rn
	FROM base
	ORDER BY arrival_month, service
)
SELECT
	arrival_month, patient_id, full_name, service, youngest_patient, oldest_patient
FROM ranked
WHERE rn = 1;

-- Average admission duration per month per service
WITH base AS (
	SELECT
		patient_id, full_name, age, arrival, departure, service,
        DATEDIFF(departure, arrival) AS duration,
        EXTRACT(MONTH FROM arrival) AS month_num,
        WEEK(arrival,4) AS week_num
	FROM patients
),
ranked AS (
	SELECT
		month_num, week_num, service,
        AVG(duration) OVER (PARTITION BY month_num, week_num, service) AS average_stay_duration,
        ROW_NUMBER() OVER (PARTITION BY month_num, week_num, service) AS rn
	FROM base
    ORDER BY month_num, week_num, service
)
SELECT
	month_num, week_num, service, average_stay_duration
FROM ranked
WHERE rn = 1
ORDER BY month_num, week_num, service;

-- Does admission duration affect patient satisfaction?
WITH base AS (
	SELECT
		service, satisfaction,
        EXTRACT(MONTH FROM arrival) AS month_num,
        WEEK(arrival, 4) AS week_num,
        DATEDIFF(departure, arrival) as duration
	FROM patients
),
calculate AS (
	SELECT
		month_num, week_num, service, duration, satisfaction,
        AVG(duration) OVER (PARTITION BY month_num, week_num, service) AS average_duration,
        AVG(satisfaction) OVER (PARTITION BY month_num, week_num, service) AS average_satisfaction
	FROM base
    ORDER BY month_num, week_num, service
),
classify AS (
	SELECT
		month_num, week_num, service, satisfaction,
        CASE
			WHEN duration >= average_duration AND satisfaction <= average_satisfaction THEN 'Affected'
            WHEN duration < average_duration AND satisfaction > average_satisfaction THEN 'Affected'
            ELSE 'Not affected'
		END AS Possibility_of_effect
	FROM calculate
)
SELECT
	SUM(Possibility_of_effect = 'Affected') AS total_affected,
    SUM(Possibility_of_effect = 'Not affected') AS total_not_affected
FROM classify;
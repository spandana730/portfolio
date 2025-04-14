-- AIRLINES TABLE
create or replace TABLE AIRLINEDELAY.PUBLIC.AIRLINES (
    OP_CARRIER_AIRLINE_ID NUMBER(38,0),
	AIRLINE_CODE VARCHAR(5) NOT NULL,
	AIRLINE_NAME VARCHAR(255),
	primary key (OP_CARRIER_AIRLINE_ID)
);

INSERT INTO AIRLINEDELAY.PUBLIC.AIRLINES (OP_CARRIER_AIRLINE_ID, Airline_Code, Airline_Name)
SELECT DISTINCT OP_CARRIER_AIRLINE_ID, Airline_Code, Airline_Name
FROM AIRLINEDELAY.PUBLIC.FLIGHT_INFO ;

Select * from airlines;

-- AIRPORTS TABLE
create or replace TABLE AIRLINEDELAY.PUBLIC.AIRPORTS (
	AIRPORT_ID NUMBER(38,0) NOT NULL,
	AIRPORT_CODE VARCHAR(3),
	primary key (AIRPORT_ID)
);

INSERT INTO AIRLINEDELAY.PUBLIC.AIRPORTS (AIRPORT_ID, AIRPORT_CODE)
SELECT DISTINCT ORIGIN_AIRPORT_ID, ORIGIN
FROM AIRLINEDELAY.PUBLIC.FLIGHT_INFO ;

Select * from AIRLINEDELAY.PUBLIC.AIRPORTS;

-- FLIGHTS TABLE
create or replace TABLE AIRLINEDELAY.PUBLIC.FLIGHTS (
	FL_DATE VARCHAR(20),
	OP_CARRIER_AIRLINE_ID NUMBER(38,0),
	TAIL_NUM VARCHAR(20),
	OP_CARRIER_FL_NUM NUMBER(38,0),
	ORIGIN_AIRPORT_ID NUMBER(38,0),
	DEST_AIRPORT_ID NUMBER(38,0),
	CRS_DEP_TIME VARCHAR(20),
	DEP_TIME VARCHAR(20),
	DEP_DELAY_NEW NUMBER(38,0),
	ARR_TIME VARCHAR(20),
	ARR_DELAY_NEW NUMBER(38,0),
	CANCELLED NUMBER(38,0),
	CANCELLATION_CODE VARCHAR(1),
	CRS_ELAPSED_TIME VARCHAR(20),
	ACTUAL_ELAPSED_TIME VARCHAR(20),
	DAY_OF_WEEK VARCHAR(20),
	PRIMARY KEY (FL_DATE, OP_CARRIER_AIRLINE_ID, TAIL_NUM, OP_CARRIER_FL_NUM),
	foreign key (OP_CARRIER_AIRLINE_ID) references AIRLINEDELAY.PUBLIC.AIRLINES(OP_CARRIER_AIRLINE_ID),
	foreign key (ORIGIN_AIRPORT_ID) references AIRLINEDELAY.PUBLIC.AIRPORTS(AIRPORT_ID),
	foreign key (DEST_AIRPORT_ID) references AIRLINEDELAY.PUBLIC.AIRPORTS(AIRPORT_ID)
);

INSERT INTO AIRLINEDELAY.PUBLIC.Flights (
    FL_DATE,
    OP_CARRIER_AIRLINE_ID,
    TAIL_NUM,
    OP_CARRIER_FL_NUM,
    ORIGIN_AIRPORT_ID,
    DEST_AIRPORT_ID,
    CRS_DEP_TIME,
    DEP_TIME,
    DEP_DELAY_NEW,
    ARR_TIME,
    ARR_DELAY_NEW,
    CANCELLED,
    CANCELLATION_CODE,
    CRS_ELAPSED_TIME,
    ACTUAL_ELAPSED_TIME,
    DAY_OF_WEEK
)
SELECT
    FL_DATE,
    OP_CARRIER_AIRLINE_ID,
    TAIL_NUM,
    OP_CARRIER_FL_NUM,
    ORIGIN_AIRPORT_ID,
    DEST_AIRPORT_ID,
    CRS_DEP_TIME,
    DEP_TIME,
    DEP_DELAY_NEW,
    ARR_TIME,
    ARR_DELAY_NEW,
    CANCELLED,
    CANCELLATION_CODE,
    CRS_ELAPSED_TIME,
    ACTUAL_ELAPSED_TIME,
    DAY_OF_WEEK
FROM AIRLINEDELAY.PUBLIC.flight_info;

ALTER TABLE flights
MODIFY COLUMN OP_CARRIER_AIRLINE_ID NUMBER(38,0);

SELECT * FROM AIRLINEDELAY.PUBLIC.FLIGHTS;

-- FLIGHTDELAY TABLE
create or replace TABLE AIRLINEDELAY.PUBLIC.FLIGHTDELAYS (
	FL_DATE VARCHAR(20),
	OP_CARRIER_AIRLINE_ID NUMBER(38,0),
	TAIL_NUM VARCHAR(20),
	OP_CARRIER_FL_NUM NUMBER(38,0),
	CARRIER_DELAY NUMBER(38,0),
	WEATHER_DELAY NUMBER(38,0),
	NAS_DELAY NUMBER(38,0),
	SECURITY_DELAY NUMBER(38,0),
	LATE_AIRCRAFT_DELAY NUMBER(38,0),
	foreign key (FL_DATE, OP_CARRIER_AIRLINE_ID, TAIL_NUM, OP_CARRIER_FL_NUM) references FLIGHTDELAY.PUBLIC.FLIGHTS(FL_DATE,OP_CARRIER_AIRLINE_ID,TAIL_NUM,OP_CARRIER_FL_NUM)
);

drop table AIRLINEDELAY.PUBLIC.FLIGHTDELAYS;

INSERT INTO FlightDelays (FL_DATE, OP_CARRIER_AIRLINE_ID, TAIL_NUM, OP_CARRIER_FL_NUM, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY)
SELECT 
    UF.FL_DATE,
    UF.OP_CARRIER_AIRLINE_ID,
    UF.TAIL_NUM,
    UF.OP_CARRIER_FL_NUM,
    UF.CARRIER_DELAY,
    UF.WEATHER_DELAY,
    UF.NAS_DELAY,
    UF.SECURITY_DELAY,
    UF.LATE_AIRCRAFT_DELAY
FROM flight_info UF
JOIN Flights F ON UF.FL_DATE = F.FL_DATE AND UF.OP_CARRIER_AIRLINE_ID = F.OP_CARRIER_AIRLINE_ID AND UF.TAIL_NUM = F.TAIL_NUM AND UF.OP_CARRIER_FL_NUM = F.OP_CARRIER_FL_NUM;

Select * from AIRLINEDELAY.PUBLIC.FLIGHTDELAYS;


ALTER TABLE flights
MODIFY COLUMN OP_CARRIER_AIRLINE_ID NUMBER(38,0);


USE database AIRLINEDELAY;

-- Find the total number of flights for each airline, showing the airline code, name, and the count of flights.
SELECT
    A.Airline_Code,
    A.Airline_Name,
    COUNT(F.OP_CARRIER_AIRLINE_ID) AS Total_Flights
FROM
    Airlines A
JOIN
    Flights F ON A.OP_CARRIER_AIRLINE_ID = F.OP_CARRIER_AIRLINE_ID
GROUP BY
    A.Airline_Code, A.Airline_Name;

 -- Calculate the average departure delay for each airline, showing the airline code, name, and the average delay.
SELECT
    A.Airline_Code,
    A.Airline_Name,
    AVG(F.DEP_DELAY_NEW) AS Average_Departure_Delay
FROM
    Airlines A
JOIN
    Flights F ON A.OP_CARRIER_AIRLINE_ID = F.OP_CARRIER_AIRLINE_ID
GROUP BY
    A.Airline_Code, A.Airline_Name;

    -- Show the count of canceled flights and the cancellation reasons.
SELECT
    CASE
        WHEN cancellation_code = 'A' THEN 'Maintenance issues'
        WHEN cancellation_code = 'B'THEN 'Weather Issues'
        WHEN cancellation_code = 'C' THEN 'National Transporation Issue'
        ELSE 'Severe Delay'
    END AS CancelLation_Reason,
    COUNT(*) AS Canceled_Flights
FROM
    Flights
WHERE
    CANCELLED = 1
GROUP BY
    CANCELLATION_CODE;
-- List the top 5 busiest airports based on the total number of departures and arrivals.

SELECT
    Airport_ID,
    Airport_Code,
    SUM(CASE WHEN Origin_Airport_ID = Airport_ID THEN 1 ELSE 0 END) AS Departures,
    SUM(CASE WHEN Dest_Airport_ID = Airport_ID THEN 1 ELSE 0 END) AS Arrivals,
    SUM(1) AS Total_Flights
FROM
    Airports A
JOIN
    Flights F ON A.Airport_ID IN (F.Origin_Airport_ID, F.Dest_Airport_ID)
GROUP BY
    Airport_ID, Airport_Code
ORDER BY
    Total_Flights DESC
LIMIT 5;


-- Show the distribution of departure delays with categories (e.g., early, on time, minor delay, moderate delay, severe delay).

SELECT
    CASE
        WHEN DEP_DELAY_NEW <= 0 THEN 'On Time'
        WHEN DEP_DELAY_NEW <= 15 THEN 'Minor Delay'
        WHEN DEP_DELAY_NEW <= 60 THEN 'Moderate Delay'
        ELSE 'Severe Delay'
    END AS Delay_Category,
    COUNT(*) AS Flights_Count
FROM
    Flights
GROUP BY
    Delay_Category
ORDER BY
    Delay_Category;


-- Calculate the average departure delay for each day of the week.


    SELECT
    DAY_OF_WEEK,
    AVG(DEP_DELAY_NEW) AS Average_Departure_Delay
FROM
    Flights
GROUP BY
    DAY_OF_WEEK
ORDER BY
    DAY_OF_WEEK;


-- Find the airline with the maximum weather delay, showing the airline code and name.

    SELECT
    A.Airline_Code,
    A.Airline_Name,
    MAX(FD.WEATHER_DELAY) AS Max_Weather_Delay
FROM
    Airlines A
JOIN
    Flights F ON A.OP_CARRIER_AIRLINE_ID = F.OP_CARRIER_AIRLINE_ID
JOIN
    FlightDelays FD ON F.FL_DATE = FD.FL_DATE AND F.OP_CARRIER_AIRLINE_ID = FD.OP_CARRIER_AIRLINE_ID AND F.OP_CARRIER_FL_NUM = FD.OP_CARRIER_FL_NUM
GROUP BY
    A.Airline_Code, A.Airline_Name
ORDER BY
    Max_Weather_Delay DESC
LIMIT 1;

-- Determine the busiest day of the week based on the total number of flights.


SELECT
    DAY_OF_WEEK,
    COUNT(*) AS Total_Flights
FROM
    Flights
GROUP BY
    DAY_OF_WEEK
ORDER BY
    Total_Flights DESC
LIMIT 1;


-- Show the count of on-time, delayed, and canceled flights for each route (origin-destination pair).

SELECT
    Origin_Airport_ID,
    DEST_AIRPORT_ID,
    COUNT(*) AS Total_Flights,
    SUM(CASE WHEN DEP_DELAY_NEW <= 0 AND ARR_DELAY_NEW <= 0 THEN 1 ELSE 0 END) AS OnTime_Flights,
    SUM(CASE WHEN DEP_DELAY_NEW > 0 OR ARR_DELAY_NEW > 0 THEN 1 ELSE 0 END) AS Delayed_Flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS Canceled_Flights
FROM
    Flights
GROUP BY
    Origin_Airport_ID, DEST_AIRPORT_ID;

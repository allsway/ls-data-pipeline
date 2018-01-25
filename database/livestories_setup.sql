CREATE DATABASE LIVESTORIES;

CREATE TABLE METRICS (
	zscore float,
	location_ref char(12),
	dimension char(245),
	indicator_id char(10)	
)


/* Set up user and give access to user  */
CREATE USER sparkaccess;
GRANT ALL PRIVILEGES ON TABLE metrics to sparkaccess;


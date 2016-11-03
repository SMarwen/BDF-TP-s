DROP TABLE msPrenomsEx;
DROP TABLE msPrenoms;

CREATE EXTERNAL TABLE msPrenomsEx(
	prenom STRING,
	gender VARCHAR(3),  
	origin VARCHAR(100),
	version DOUBLE)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\073' STORED AS TEXTFILE 
  location '/user/msallem/prenoms';

CREATE TABLE msPrenoms(
	prenom STRING,	
	gender VARCHAR(3), 
	origin VARCHAR(100),
	version DOUBLE)
  ROW FORMAT DELIMITED
  STORED AS ORC;

INSERT OVERWRITE TABLE msPrenoms SELECT * FROM msPrenomsEx;

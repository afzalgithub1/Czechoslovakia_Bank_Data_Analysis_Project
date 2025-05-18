create database end_to_end_project;

CREATE OR REPLACE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants	INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;


CREATE OR REPLACE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(40),
`Date` DATE ,
Account_type VARCHAR(40),
Card_Assigned VARCHAR(20),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);


CREATE OR REPLACE TABLE `ORDER`(
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE OR REPLACE TABLE LOAN(
loan_id	INT ,
account_id	INT,
`Date`	DATE,
amount	INT,
duration	INT,
payments	INT,
`status` VARCHAR(35),
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);


CREATE OR REPLACE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
`Date`	DATE,
`Type`	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
`account` INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));


CREATE OR REPLACE TABLE CLIENT(
client_id	INT PRIMARY KEY,
Sex	CHAR(10),
Birth_date	DATE,
district_id INT,
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);


CREATE OR REPLACE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
`type` CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);


CREATE OR REPLACE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
`type` CHAR(10)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);

--------------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::688930150039:role/snowbankrole' -- role ARN
STORAGE_ALLOWED_LOCATIONS =('s3://czechbankdata07'); -- bucket name

desc integration s3_int;

CREATE OR REPLACE FILE FORMAT CSV_BANK
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE BANK
URL ='s3://czechbankdata07'
--credentials=(aws_key_id='AKIAXQKR3H3PSG72XFMK'aws_secret_key='eKL6a6FjlQHic4s8Ne712Aelzg2ou4j6tNsVvFq5')
file_format = CSV_BANK
storage_integration = s3_int;

LIST @BANK;

SHOW STAGES;

-----------------------------------------

--CREATING SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE

--The AUTO_INGEST=true parameter specifies to read 
--- event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.


CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."DISTRICT" --yourdatabase -- your schema ---your table
FROM '@BANK/district/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."ACCOUNT"
FROM '@BANK/Account/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_TXNS AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."TRANSACTIONS"
FROM '@BANK/trnx/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISP AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."DISPOSITION"
FROM '@BANK/disp/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CARD AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."CARD"
FROM '@BANK/card/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ORDER_LIST AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."`ORDER`"
FROM '@BANK/order/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_LOAN AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."LOAN"
FROM '@BANK/loan/'
FILE_FORMAT = CSV_BANK;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CLIENT AUTO_INGEST = TRUE AS
COPY INTO "END_TO_END_PROJECT"."PUBLIC"."CLIENT"
FROM '@BANK/client/'
FILE_FORMAT = CSV_BANK;

show pipes;

--------------------------------------

SELECT count(*) FROM DISTRICT;
SELECT count(*) FROM ACCOUNT;
SELECT count(*) FROM TRANSACTIONS;
SELECT count(*) FROM DISPOSITION;
SELECT count(*) FROM CARD;
SELECT count(*) FROM `ORDER`;
SELECT count(*) FROM LOAN;
SELECT count(*) FROM CLIENT;

ALTER PIPE BANK_SNOWPIPE_DISTRICT refresh;

ALTER PIPE BANK_SNOWPIPE_ACCOUNT refresh;

ALTER PIPE BANK_SNOWPIPE_TXNS refresh;

ALTER PIPE BANK_SNOWPIPE_DISP refresh;

ALTER PIPE BANK_SNOWPIPE_CARD refresh;

ALTER PIPE BANK_SNOWPIPE_ORDER_LIST refresh;

ALTER PIPE BANK_SNOWPIPE_LOAN refresh;

ALTER PIPE BANK_SNOWPIPE_CLIENT refresh;
------------------------------------------------------------------------------------------------------------------------
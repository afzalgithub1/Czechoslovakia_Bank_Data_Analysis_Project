-----------------------------------------Ad-hoc Data Analysis-----------------------------------

/*The Czechoslovakia Bank wants to analyse its financial data to gain insights and make informed decisions. The bank needs to identify trends, patterns, and potential risks in its financial operations. They also want to explore the possibility of introducing new financial products or services based on their analysis.
The bank has identified the following questions as important for their analysis:*/

SELECT * FROM DISTRICT;
SELECT * FROM ACCOUNT;
SELECT * FROM TRANSACTIONS;
SELECT * FROM DISPOSITION;
SELECT * FROM CARD;
SELECT * FROM `ORDER`;
SELECT * FROM LOAN;
SELECT * FROM CLIENT;


-- -----------------1. What is the demographic profile of the bank's clients and how does it vary across districts?----

-- GENDER DISTRIBUTION

select
    sum(case when SEX = 'Male' then 1 end) as male_client,
    sum(case when SEX = 'Female' then 1 end) as female_client
    from client;


-- Creating a banking demographic KPI.

create or replace table czech_bank_demographic_kpi as
select cl.district_id, ds.district_name, ds.average_salary,
round(avg(cl.age),0) as average_age,
sum(case when SEX = 'Male' then 1 end) as male_client,
    sum(case when SEX = 'Female' then 1 end) as female_client,
round(female_client/male_client*100,2) as female_to_male_ratio,
count(*) as total_client

from client as cl
inner join district as ds
on cl.district_id = ds.district_code
group by 1,2,3
order by 1;

select * from czech_bank_demographic_kpi;


--ASSUMING EVERY LAST MONTH CUSTOMER ACCOUNT IS GETTING TRANSACTED

CREATE OR REPLACE TABLE ACC_LATEST_TXNS_WITH_BALANCE 
AS
SELECT LTD.*,TXN.BALANCE
FROM TRANSACTIONS AS TXN
INNER JOIN 
(
   SELECT ACCOUNT_ID,YEAR("`DATE`") AS TXN_YEAR,
   MONTH("`DATE`") AS TXN_MONTH,
   MAX("`DATE`") AS LATEST_TXN_DATE
   FROM TRANSACTIONS
   GROUP BY 1,2,3
   ORDER BY 1,2,3

) AS LTD ON TXN.ACCOUNT_ID = LTD.ACCOUNT_ID AND TXN."`DATE`" = LTD.LATEST_TXN_DATE
WHERE TXN.`TYPE` = 'Credit' -- this is the assumptions am having : month end txn data is credit
ORDER BY TXN.ACCOUNT_ID,LTD.TXN_YEAR,LTD.TXN_MONTH;

select * from ACC_LATEST_TXNS_WITH_BALANCE;


------Final BANKING_KPI------------

CREATE OR REPLACE TABLE BANKING_KPI AS
SELECT  ALWB.TXN_YEAR , ALWB.TXN_MONTH,T.BANK,A.ACCOUNT_TYPE,

COUNT(DISTINCT ALWB.ACCOUNT_ID) AS TOT_ACCOUNT, 
COUNT(DISTINCT T.TRANS_ID) AS TOT_TXNS,
COUNT(CASE WHEN T.`TYPE` = 'Credit' THEN 1 END) AS DEPOSIT_COUNT ,
COUNT(CASE WHEN T.`TYPE` = 'Withdrawal' THEN 1 END) AS WITHDRAWAL_COUNT,

SUM(ALWB.BALANCE) AS TOT_BALANCE,

ROUND((DEPOSIT_COUNT / TOT_TXNS) * 100,2)  AS DEPOSIT_PERC ,
ROUND((WITHDRAWAL_COUNT / TOT_TXNS) * 100,2) AS WITHDRAWAL_PERC ,
NVL(TOT_BALANCE / TOT_ACCOUNT,0) AS AVG_BALANCE,

ROUND(TOT_TXNS/TOT_ACCOUNT,0) AS TPA

FROM TRANSACTIONS AS T
INNER JOIN  ACC_LATEST_TXNS_WITH_BALANCE AS ALWB ON T.ACCOUNT_ID = ALWB.ACCOUNT_ID
LEFT OUTER JOIN  ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

SELECT * from BANKING_KPI;
select count(*) from BANKING_KPI; -- 3887 rows

-- just to check baking KPI output
/*select count(*) from (
SELECT  distinct ALWB.TXN_YEAR , ALWB.TXN_MONTH,T.BANK,A.ACCOUNT_TYPE,


FROM TRANSACTIONS AS T
INNER JOIN  ACC_LATEST_TXNS_WITH_BALANCE AS ALWB ON T.ACCOUNT_ID = ALWB.ACCOUNT_ID
LEFT OUTER JOIN  ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
ORDER BY 1,2,3,4) as countaaa;*/


---------------------------------------------------------------------------------------------------------

-- -----------2. How the banks have performed over the years.Give their detailed analysis month wise and year wise?----

SELECT * from BANKING_KPI;
select * from transactions;

-- transactions and account based analysis
select BANK, TXN_MONTH, TXN_YEAR, TOT_ACCOUNT,
round(TOT_BALANCE,2) as total_balance,
round(AVG_BALANCE,2) as average_balance,
TPA
FROM BANKING_KPI
order by 3,2,1;


-- MONTH ON MONTH TRANSACTION TREND (MOM)-------------

select TXN_MONTH, TXN_YEAR, bank, TOT_TXNS,
        lag(TOT_TXNS) over(partition by bank order by TXN_YEAR, TXN_MONTH asc) as prev_month_txn,
        round((TOT_TXNS-prev_month_txn)*100/nullif(prev_month_txn,0),2) as mom_growth_pct
        from BANKING_KPI;

-- YEAR ON YEAR TRANSACTION TREND (YOY)

with yearly_agg as 
    (
        select TXN_YEAR, bank, sum(TOT_TXNS) as yearly_trxn
        from banking_kpi
        group by TXN_YEAR,bank
                    )

    select TXN_YEAR, bank, yearly_trxn,
        lag(yearly_trxn) over(partition by bank order by TXN_YEAR asc) as prev_YEAR_txn,
        round((yearly_trxn-prev_YEAR_txn)*100/nullif(prev_YEAR_txn,0),2) as YOY_growth_pct,
        from yearly_agg
        order by bank;
    
-- YOY AVRG BALANCE TREND(YOY)

with yearly_balance_all as 
    (
        select TXN_YEAR, bank, avg(TOT_BALANCE) as yearly_balance
        from banking_kpi
        group by TXN_YEAR,bank
                    )

    select TXN_YEAR, bank, yearly_balance,
        lag(yearly_balance) over(partition by bank order by TXN_YEAR asc) as prev_YEAR_avg_balance,
        round((yearly_balance-prev_YEAR_avg_balance)*100/nullif(prev_YEAR_avg_balance,0),2) as YOY_avg_balance_growth_pct,
        from yearly_balance_all
        order by bank;
--------------------------------------------------------------------------------------------------------

-- ---------3. What are the most common types of accounts and how do they differ in terms of usage and profitability?---

select * from account;
select * from transactions;

-- type of accounts and its usage

create or replace table account_usage as
select AC.ACCOUNT_TYPE,
        count(*) as trxn_count_by_account, sum(tr.balance) as total_balance_by_acc,
       sum(tr.amount) as trxn_volume_vy_acc
from TRANSACTIONS AS TR
INNER JOIN ACCOUNT AS AC
ON TR.ACCOUNT_ID = AC.ACCOUNT_ID
GROUP BY 1
order by 1;

select * from account_usage;

-- PROFITABILITY ON BASED CONDITION

-- CONDITION - give bonus on yearly balance based average balnce for a year for account and account type
-- here we are giving bonus to salary account

CREATE OR REPLACE TABLE BONUS_SALRIED_ACCOUNT AS
WITH data_account AS (
    SELECT 
        ac.account_id, 
        ac.account_type, 
        YEAR(tr.`date`) AS year_x,
        ROUND(SUM(tr.balance), 2) AS year_blnc,
        ROUND(AVG(tr.balance), 2) AS avg_balance
    FROM transactions AS tr
    INNER JOIN account AS ac
        ON tr.account_id = ac.account_id
    GROUP BY ac.account_id, ac.account_type, YEAR(tr.`date`)
)
SELECT 
    account_id, 
    year_x, 
    avg_balance,
    year_blnc,
    account_type,
    CASE 
    WHEN avg_balance > 80000 AND account_type = 'Salary account' THEN year_blnc * 0.05
    WHEN avg_balance > 60000 AND avg_balance <= 80000 AND account_type = 'Salary account' THEN year_blnc * 0.04
    WHEN avg_balance > 40000 AND avg_balance <= 60000 AND account_type = 'Salary account' THEN year_blnc * 0.03
    WHEN avg_balance > 20000 AND avg_balance <= 40000 AND account_type = 'Salary account' THEN year_blnc * 0.02
    ELSE avg_balance
    END AS bonus_salaried_account
FROM data_account
order by AVG_BALANCE desc;

SELECT * FROM BONUS_SALRIED_ACCOUNT; --WHERE ACCOUNT_TYPE = 'Salary account';
-----------------------------------------------------------------------------------------

-- ----- 4. Which types of cards are most frequently used by the bank's clients.----------------------

select * from card; -- 892 rows
select * from client; -- 5369 rows
select * from disposition; -- 5369 rows
select count(*) from transactions;
select * from transactions; -- 1048575 rows
select distinct CLIENT_ID from client;

-- unique clients usage of each card type at each bank

with client_card as(

select distinct cl.client_id, trxn.account_id, ds.disp_id, cd.card_id,
cd."`TYPE`" as card_type, cd.issued as card_issue_date,
trxn.bank,trxn.balance, trxn.amount as trxn_amount

from transactions as trxn
inner join disposition as ds on trxn.account_id = ds.account_id
inner join client as cl on ds.client_id = cl.client_id
inner join card as cd on ds.disp_id = cd.disp_id
)
    select card_type, bank, count(distinct client_id) as card_user
    from client_card
    group by 1,2
    order by 1,3 desc;

-- client-transaction pairs exist for each card type at each bank

with client_card as(

select distinct cl.client_id, trxn.account_id, ds.disp_id, cd.card_id,
cd."`TYPE`" as card_type, cd.issued as card_issue_date,
trxn.bank,trxn.balance, trxn.amount as trxn_amount

from transactions as trxn
inner join disposition as ds on trxn.account_id = ds.account_id
inner join client as cl on ds.client_id = cl.client_id
inner join card as cd on ds.disp_id = cd.disp_id
)
    select card_type, bank, count(client_id) as card_user
    from client_card
    group by 1,2
    order by 1,3 desc;

-- count of transactions for each card and bank

WITH card_transactions AS (
    SELECT 
        cd."`TYPE`" AS card_type,
        trxn.bank,
        trxn.trans_id  -- Assuming there's a unique transaction_id column
    FROM transactions AS trxn
    INNER JOIN disposition AS ds ON trxn.account_id = ds.account_id
    INNER JOIN card AS cd ON ds.disp_id = cd.disp_id
)
SELECT 
    card_type,
    bank,
    COUNT(trans_id) AS transaction_count
FROM card_transactions
GROUP BY card_type, bank
ORDER BY transaction_count DESC;

-- TRANSACTIONS  METRICS AS PER CLIEN AND CARD

create or replace table trxn_card_per_client as
WITH card_activity AS (
    SELECT 
        cd."`TYPE`" AS card_type,
        trxn.bank,
        cl.client_id,
        trxn.trans_id
    FROM transactions AS trxn
    INNER JOIN disposition AS ds ON trxn.account_id = ds.account_id
    INNER JOIN client AS cl ON ds.client_id = cl.client_id
    INNER JOIN card AS cd ON ds.disp_id = cd.disp_id
)
SELECT 
    card_type,
    bank,
    COUNT(DISTINCT client_id) AS unique_clients,
    COUNT(DISTINCT trans_id) AS transaction_count,
    ROUND(COUNT(client_id) * 1.0 / COUNT(DISTINCT client_id), 2) AS avg_transactions_per_client
FROM card_activity
GROUP BY card_type, bank
ORDER BY transaction_count DESC;

select * from trxn_card_per_client;

--5. What are the major expenses of the bank and how can they be reduced to improve profitability?

-- considering the major expense is processing charge on each transaction based on type of transaction, amount, and operation.
-- below created a hypothetical condition

/*
| Type       | Base Cost |
| ---------- | --------- |
| Credit     | ₹2        |
| Withdrawal | ₹3        |

| Amount Range      | Additional Cost |
| ----------------- | --------------- |
| ₹0 – ₹1,000       | ₹0.50           |
| ₹1,001 – ₹10,000  | ₹1.00           |
| ₹10,001 – ₹50,000 | ₹2.00           |
| Above ₹50,000     | ₹3.00           |

| Operation                       | Adjustment to Total |
| ------------------------------- | ------------------- |
| Withdrawal in cash              | +₹1.00              |
| Remittance to Another Bank      | +₹2.00              |
| Interest Credit                 | -₹1.00              |
| Electronic funds transfer (EFT) | +₹0.50              |
| Credit in cash                  | +₹1.00              |
| Credit card withdrawal          | +₹2.00              |*/

select * from banking_kpi;
select * from transactions;


create or replace table trxns_processing_cost as
WITH cost_component AS (
    SELECT 
        trans_id, 
        `type`, 
        operation, 
        amount,

        -- Base cost with COALESCE
        COALESCE(
            CASE
                WHEN `type` = 'Withdrawal' THEN 3 
                ELSE 2 
            END, 
        0) AS base_cost,

        -- Amount transaction cost with COALESCE
        COALESCE(
            CASE
                WHEN amount <= 1000 THEN 0.50
                WHEN amount <= 10000 THEN 1
                WHEN amount <= 50000 THEN 2
                ELSE 3
            END, 
        0) AS amount_trxn_cost,

        -- Operation adjustment cost with COALESCE
        COALESCE(
            CASE
                WHEN operation = 'Withdrawal in cash' THEN 1
                WHEN operation = 'Remittance to Another Bank' THEN 2
                WHEN operation = 'Interest Credit' THEN -1
                WHEN operation = 'Electronic funds transfer (EFT)' THEN 0.50
                WHEN operation = 'Credit in cash' THEN 1
                WHEN operation = 'Credit card withdrawal' THEN 2
                ELSE 0
            END, 
        0) AS operation_adj_cost

    FROM transactions
)

SELECT 
    trans_id, 
    `type`, 
    operation, 
    amount,
    base_cost + amount_trxn_cost + operation_adj_cost AS processing_cost
FROM cost_component;

select * from trxns_processing_cost;

/* 
INFERENCES-----

Customer Education:

Run campaigns promoting low-cost transaction methods like EFT or digital wallets.

Product Redesign:

Bundle low-cost services and offer them in packages to encourage efficient usage.

Process Automation:

Automate high-frequency operations to reduce per-transaction manual handling cost.

Transaction Monitoring:

Identify clients with frequent high-cost transactions and offer tailored alternatives.

*/


-- 6. What is the bank’s loan portfolio and how does it vary across different purposes and client segments.

select * from loan;
select * from client;
select * from czech_bank_demographic_kpi;
select * from transactions;
select * from account;

create or replace table loan_bank_kpi as

    select trxn.bank, loan_kpi.loan_year, loan_kpi.loan_month,
        ac.account_type, loan_kpi.loan_status, sum(loan_kpi.loan_amount) as loan_aggregate
    from
    transactions as trxn
    inner join (
            select year(`DATE`) as loan_year, month(`DATE`) as loan_month,
            account_id,`STATUS` as loan_status,sum(amount) as loan_amount
            from loan
            group by 1,2,3,4) as loan_kpi
            on trxn.account_id = loan_kpi.account_id

            inner join account as ac on loan_kpi.account_id = ac.account_id
            
    group by 1,2,3,4,5
    order by 1,2,3,6;

select * from loan_bank_kpi order by bank, loan_year, loan_month, loan_aggregate;
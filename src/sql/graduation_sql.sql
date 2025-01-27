/*Я применил две функции к octet_length и LENGTH к строковым данным из PG, чтобы посмотреть сколько байт и символов они занимают
 * Подумал, что в Postgresql слишком большие стоят ограничения по длине/байтам в VARCHAR, поэтому сделал поменьше
*/
-- Числовые делаю INTEGER, потому что данные целочисленные
-- NOT NULL не ставил, потому что данные нужны AS IS

DROP TABLE IF EXISTS STV2024111513__STAGING.transactions;

CREATE TABLE IF NOT EXISTS STV2024111513__STAGING.transactions(
operation_id VARCHAR(40) PRIMARY KEY, -- поставил здесь VARCHAR, потому что в PG тоже varchar, а не UUID, хотя записи выглядят так, как будто это UUID
account_number_from	INTEGER,
account_number_to INTEGER,	
currency_code INTEGER,	
country	VARCHAR(15),
status VARCHAR(20),	
transaction_type VARCHAR(30),	
amount INTEGER,	
transaction_dt TIMESTAMP(3) -- в Postgres у меня до миллисекунд, значит и здесь будет до миллисекунд
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY EXTRACT(DAY FROM transaction_dt); -- решил сделать партицию по дням, потому что часов или минут было бы слишком много

--Проекция по датам
DROP PROJECTION IF EXISTS transactions_dt_projection;

CREATE PROJECTION transactions_dt_projection as
SELECT operation_id, transaction_dt
FROM STV2024111513__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;



DROP TABLE IF EXISTS STV2024111513__STAGING.currencies;


CREATE TABLE IF NOT EXISTS STV2024111513__STAGING.currencies(
currency_code INTEGER,
currency_code_with INTEGER,
date_update	TIMESTAMP(3), -- в Postgres у меня до миллисекунд, значит и здесь будет до миллисекунд
currency_with_div NUMERIC(5,3)
)
ORDER BY date_update
PARTITION BY EXTRACT(DAY FROM date_update);--добавил партицию по дням

--проверка, что данные в таблицах
SELECT COUNT(1) FROM STV2024111513__STAGING.currencies;
SELECT COUNT(1) FROM STV2024111513__STAGING.transactions;




DROP TABLE IF EXISTS STV2024111513__DWH.global_metrics;

CREATE TABLE IF NOT EXISTS STV2024111513__DWH.global_metrics(
date_update TIMESTAMP(3) NOT NULL,
currency_from INTEGER NOT NULL,
amount_total INTEGER NOT NULL,
cnt_transactions INTEGER NOT NULL,
avg_transactions_per_account FLOAT NOT NULL,
cnt_accounts_make_transactions INTEGER NOT NULL
)
ORDER BY currency_from;


--Создаю копию таблицы global_metrics для инкремента
CREATE TABLE STV2024111513__DWH.global_metrics_inc LIKE STV2024111513__DWH.global_metrics INCLUDING PROJECTIONS;
--Очищаю данные инкремента. Выбрал TRUNCATE, потому что он будт выполняться только с заполнением STG-слоя
TRUNCATE TABLE STV2024111513__DWH.global_metrics_inc;

-- Вставляю данные в инкермент, чтобы потом оттуда взять данные для витрины
INSERT INTO STV2024111513__DWH.global_metrics_inc(date_update, currency_from, amount_total, cnt_transactions,
                                                  avg_transactions_per_account, cnt_accounts_make_transactions)
SELECT NOW() AS date_update,
	   t.currency_code AS currency_from, 
	   SUM(t.amount * c.currency_with_div) AS amount_total,
	   COUNT(*) AS cnt_transactions,	
       AVG(t.amount) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM STV2024111513__STAGING.transactions t
LEFT JOIN STV2024111513__STAGING.currencies c ON t.currency_code = c.currency_code
WHERE c.currency_code_with = 420 
AND t.status = 'done'
AND t.account_number_from IN( SELECT t2.account_number_from FROM STV2024111513__STAGING.transactions AS t2 WHERE t2.account_number_from!=-1)
AND t.transaction_type != 'authorisation'
GROUP BY t.currency_code
UNION
SELECT NOW() AS date_update,
	   t.currency_code AS currency_from, 
	   SUM(t.amount * c.currency_with_div) AS amount_total,
	   COUNT(*) AS cnt_transactions,	
       AVG(t.amount) AS avg_transactions_per_account,
       COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM STV2024111513__STAGING.transactions t
LEFT JOIN STV2024111513__STAGING.currencies c ON t.currency_code = c.currency_code
WHERE c.currency_code=420
AND t.status = 'done'
AND t.account_number_from IN(SELECT t2.account_number_from FROM STV2024111513__STAGING.transactions AS t2 WHERE t2.account_number_from!=-1)
AND t.transaction_type != 'authorisation'
GROUP BY t.currency_code
ORDER BY currency_from;
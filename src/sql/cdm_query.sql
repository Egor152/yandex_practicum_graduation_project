--Заполняю витрину данных

MERGE INTO STV2024111513__DWH.global_metrics AS trgt
USING STV2024111513__DWH.global_metrics_inc AS src
ON
trgt.currency_from = src.currency_from
WHEN MATCHED AND( --условие, чтобы разница между данными меньше 1 дня для апдейта
DATEDIFF('day', trgt.date_update, src.date_update) < 1)
THEN UPDATE SET
date_update = src.date_update,
currency_from = src.currency_from,
amount_total = src.amount_total,
cnt_transactions = src.cnt_transactions,
avg_transactions_per_account = src.avg_transactions_per_account,
cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED 
THEN INSERT(date_update,currency_from, amount_total,cnt_transactions,	
            avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES(src.date_update, src.currency_from, src.amount_total, src.cnt_transactions,
       src.avg_transactions_per_account, src.cnt_accounts_make_transactions);
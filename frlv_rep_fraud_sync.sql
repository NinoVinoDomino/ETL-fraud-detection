INSERT INTO deaian.trsh_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt, processed_dt)
-- 1. Совершение операции при просроченном или заблокированном паспорте.
SELECT		tr.trans_date AS event_dt
			,cl.passport_num AS passport
			,CONCAT_WS(' ', cl.first_name, cl.last_name, cl.patronymic) AS fio
			,cl.phone
			,1 AS event_type
			,CAST(tr.create_dt AS DATE) AS report_dt
			,NOW() AS processed_dt
FROM		deaian.trsh_dwh_fact_transaction AS tr
			INNER JOIN deaian.trsh_dwh_dim_cards_hist AS c ON tr.card_num = c.card_num
				AND tr.trans_date BETWEEN c.effective_from AND c.effective_to
			INNER JOIN deaian.trsh_dwh_dim_accounts_hist AS ac ON c.account_num = ac.account_num
				AND tr.trans_date BETWEEN ac.effective_from AND ac.effective_to
			INNER JOIN deaian.trsh_dwh_dim_clients_hist AS cl ON ac.client = cl.client_id
				AND tr.trans_date BETWEEN cl.effective_from AND cl.effective_to
WHERE		(tr.trans_date > cl.passport_valid_to
			OR EXISTS(	SELECT		1
						FROM		deaian.trsh_dwh_fact_passport_blacklist AS p
						WHERE		cl.passport_num = p.passport_num
									AND tr.trans_date > p.entry_dt))
			AND tr.create_dt > COALESCE((	SELECT		MAX(report_dt)
											FROM 		deaian.trsh_rep_fraud)
										,TO_DATE('1800-01-01', 'YYYY-MM-DD'))
UNION ALL
-- 2. Совершение операции при недействующем договоре.
SELECT		tr.trans_date AS event_dt
			,cl.passport_num AS passport
			,CONCAT_WS(' ', cl.first_name, cl.last_name, cl.patronymic) AS fio
			,cl.phone
			,2 AS event_type
			,CAST(tr.create_dt AS DATE) AS report_dt
			,NOW() AS processed_dt
FROM		deaian.trsh_dwh_fact_transaction AS tr
			INNER JOIN deaian.trsh_dwh_dim_cards_hist AS c ON tr.card_num = c.card_num
				AND tr.trans_date BETWEEN c.effective_from AND c.effective_to
			INNER JOIN deaian.trsh_dwh_dim_accounts_hist AS ac ON c.account_num = ac.account_num
				AND tr.trans_date BETWEEN ac.effective_from AND ac.effective_to
				AND tr.trans_date > ac.valid_to
			INNER JOIN deaian.trsh_dwh_dim_clients_hist AS cl ON ac.client = cl.client_id
				AND tr.trans_date BETWEEN cl.effective_from AND cl.effective_to
WHERE		tr.create_dt > COALESCE((	SELECT		MAX(report_dt)
										FROM 		deaian.trsh_rep_fraud)
									,TO_DATE('1800-01-01', 'YYYY-MM-DD'))
UNION ALL
-- 3. Совершение операций в разных городах в течение одного часа.
SELECT		trans_date AS event_dt
			,passport_num
			,CONCAT_WS(' ', first_name, last_name, patronymic) AS fio
			,phone
			,3 AS event_type
			,CAST(create_dt AS DATE) AS report_dt
			,NOW() AS processed_dt
FROM		(
			SELECT		cl.client_id
						,cl.first_name
						,cl.last_name
						,cl.patronymic
						,cl.passport_num
						,cl.phone
						,t.terminal_city
						,tr.trans_date
						,LAG(t.terminal_city) OVER(PARTITION BY cl.client_id ORDER BY tr.trans_date) AS prv_city
						,LAG(tr.trans_date) OVER(PARTITION BY cl.client_id ORDER BY tr.trans_date) AS prv_dt
						,tr.create_dt
			FROM		deaian.trsh_dwh_fact_transaction AS tr
						INNER JOIN deaian.trsh_dwh_dim_cards_hist AS c ON tr.card_num = c.card_num
							AND tr.trans_date BETWEEN c.effective_from AND c.effective_to
						INNER JOIN deaian.trsh_dwh_dim_accounts_hist AS ac ON c.account_num = ac.account_num
							AND tr.trans_date BETWEEN ac.effective_from AND ac.effective_to
						INNER JOIN deaian.trsh_dwh_dim_clients_hist AS cl ON ac.client = cl.client_id
							AND tr.trans_date BETWEEN cl.effective_from AND cl.effective_to
						INNER JOIN deaian.trsh_dwh_dim_terminals_hist AS t ON t.terminal_id = tr.terminal
							AND tr.trans_date BETWEEN t.effective_from AND t.effective_to
						) AS a
WHERE		terminal_city <> prv_city
			AND trans_date < prv_dt + INTERVAL '1 HOUR'
			AND create_dt > COALESCE((	SELECT		MAX(report_dt)
										FROM 		deaian.trsh_rep_fraud)
									,TO_DATE('1800-01-01', 'YYYY-MM-DD'))
UNION ALL
/* 4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций
со следующим шаблоном – каждая последующая меньше предыдущей, при этом
отклонены все кроме последней. Последняя операция (успешная) в такой цепочке
считается мошеннической. */
SELECT		trans_date AS event_dt
			,passport_num
			,CONCAT_WS(' ', first_name, last_name, patronymic) AS fio
			,phone
			,4 AS event_type
			,CAST(create_dt AS DATE) AS report_dt
			,NOW() AS processed_dt
FROM		(
			SELECT		*
						,MIN(CASE WHEN amt < prv_amt THEN 1 ELSE 0 END) OVER(PARTITION BY client_id, card_num ORDER BY trans_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS reducion
			FROM		(
						SELECT		cl.client_id
									,tr.card_num
									,cl.first_name
									,cl.last_name
									,cl.patronymic
									,cl.passport_num
									,cl.phone
									,tr.trans_date
									,tr.trans_id
									,tr.oper_type
									,tr.oper_result
									,tr.amt
									,tr.create_dt
									,LAG(tr.amt) OVER(PARTITION BY cl.client_id, tr.card_num ORDER BY tr.trans_date) AS prv_amt
									,MIN(tr.trans_date) OVER(PARTITION BY cl.client_id, tr.card_num ORDER BY tr.trans_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_dt
									,SUM(CASE WHEN tr.oper_type IN ('WITHDRAW', 'PAYMENT') AND tr.oper_result = 'REJECT' THEN 1 ELSE 0 END) OVER(PARTITION BY cl.client_id, tr.card_num ORDER BY tr.trans_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS oper
						FROM		deaian.trsh_dwh_fact_transaction AS tr
									INNER JOIN deaian.trsh_dwh_dim_cards_hist AS c ON tr.card_num = c.card_num
										AND tr.trans_date BETWEEN c.effective_from AND c.effective_to
									INNER JOIN deaian.trsh_dwh_dim_accounts_hist AS ac ON c.account_num = ac.account_num
										AND tr.trans_date BETWEEN ac.effective_from AND ac.effective_to
									INNER JOIN deaian.trsh_dwh_dim_clients_hist AS cl ON ac.client = cl.client_id
										AND tr.trans_date BETWEEN cl.effective_from AND cl.effective_to
									) AS a
						) AS b
WHERE		oper_type IN ('WITHDRAW', 'PAYMENT')
			AND oper_result = 'SUCCESS'
			AND oper = 3
			AND trans_date < min_dt + INTERVAL '20 MINUTE'
			AND reducion = 1
			AND create_dt > COALESCE((	SELECT		MAX(report_dt)
										FROM 		deaian.trsh_rep_fraud)
									,TO_DATE('1800-01-01', 'YYYY-MM-DD'));

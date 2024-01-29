--------------------------------------------------
-- 1. Dropping existsing objects
--------------------------------------------------
DROP TABLE IF EXISTS deaian.trsh_stg_passport_blacklist;
DROP TABLE IF EXISTS deaian.trsh_stg_transactions;
DROP TABLE IF EXISTS deaian.trsh_stg_terminals;
DROP TABLE IF EXISTS deaian.trsh_stg_cards;
DROP TABLE IF EXISTS deaian.trsh_stg_cards_del;
DROP TABLE IF EXISTS deaian.trsh_stg_accounts;
DROP TABLE IF EXISTS deaian.trsh_stg_accounts_del;
DROP TABLE IF EXISTS deaian.trsh_stg_clients;
DROP TABLE IF EXISTS deaian.trsh_stg_clients_del;
DROP TABLE IF EXISTS deaian.trsh_meta_core_table_mapping;
DROP TABLE IF EXISTS deaian.trsh_meta_etl_update;
DROP SEQUENCE IF EXISTS deaian.trsh_etl_run;
DROP TABLE IF EXISTS deaian.trsh_meta_etl_run_log;
DROP TABLE IF EXISTS deaian.trsh_rep_fraud;
DROP TABLE IF EXISTS deaian.trsh_dwh_fact_passport_blacklist;
DROP TABLE IF EXISTS deaian.trsh_dwh_fact_transaction;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_terminals_hist;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_cards_hist;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_accounts_hist;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_clients_hist;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_frauds;


--------------------------------------------------
-- 2. Creating new objects
--------------------------------------------------
CREATE TABLE deaian.trsh_meta_etl_update(
	schema_name VARCHAR(50) NOT NULL
	,table_name VARCHAR(50) NOT NULL
	,max_update_dt DATE NOT NULL DEFAULT(TO_DATE('1800-01-01', 'YYY-MM-DD'))
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_meta_etl_update PRIMARY KEY(schema_name, table_name)
	);

CREATE SEQUENCE deaian.trsh_etl_run;

CREATE TABLE deaian.trsh_meta_etl_run_log(
	run_id INT NOT NULL
	,schema_name VARCHAR(50) NOT NULL
	,table_name VARCHAR(50) NOT NULL
	,rows_deleted INT NOT NULL DEFAULT 0
	,rows_updated INT NOT NULL DEFAULT 0
	,rows_inserted INT NOT NULL DEFAULT 0
	,run_start_dt TIMESTAMP NOT NULL
	,run_end_dt TIMESTAMP NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_meta_etl_run_log PRIMARY KEY(run_id, schema_name, table_name)
	);

CREATE TABLE deaian.trsh_meta_core_table_mapping(
	target_schema_name VARCHAR(50) NOT NULL
	,target_table_name VARCHAR(50) NOT NULL
	,target_columns VARCHAR(50)[] NOT NULL
	,target_keys VARCHAR(50)[] NOT NULL
	,scd INT NOT NULL
	,source_schema_name VARCHAR(50) NOT NULL
	,source_table_name VARCHAR(50) NOT NULL
	,source_columns VARCHAR(50)[] NOT NULL
	,source_keys VARCHAR(50)[] NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_meta_core_table_mapping PRIMARY KEY(target_schema_name, target_table_name)
	,CONSTRAINT fk_trsh_meta_core_table_mapping FOREIGN KEY(source_schema_name, source_table_name) REFERENCES deaian.trsh_meta_etl_update(schema_name, table_name)
	);

CREATE TABLE deaian.trsh_stg_passport_blacklist(
	date DATE NULL
	,passport VARCHAR(15) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_terminals(
	terminal_id VARCHAR(10) NULL
	,terminal_type VARCHAR(10) NULL
	,terminal_city VARCHAR(200) NULL
	,terminal_address VARCHAR(255) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_transactions(
	transaction_id VARCHAR(20) NULL
	,transaction_date TIMESTAMP NULL
	,amount DECIMAL(18,2) NULL
	,card_num VARCHAR(19) NULL
	,oper_type VARCHAR(20) NULL
	,oper_result VARCHAR(10) NULL
	,terminal VARCHAR(10) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_clients(
	client_id VARCHAR(10) NULL
	,last_name VARCHAR(20) NULL
	,first_name VARCHAR(20) NULL
	,patronymic VARCHAR(20) NULL
	,date_of_birth DATE NULL
	,passport_num VARCHAR(15) NULL
	,passport_valid_to DATE NULL
	,phone VARCHAR(16) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_clients_del(
	client_id VARCHAR(10) NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_accounts(
	account VARCHAR(20) NULL
	,valid_to DATE NULL
	,client VARCHAR(10) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_accounts_del(
	account VARCHAR(20) NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_cards(
	card_num VARCHAR(19) NULL
	,account VARCHAR(20) NULL
	,create_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_stg_cards_del(
	card_num VARCHAR(19) NULL
	,processed_dt TIMESTAMP NOT NULL
	);

CREATE TABLE deaian.trsh_dwh_fact_passport_blacklist(
	passport_num VARCHAR(15) NOT NULL
	,entry_dt DATE NULL
	,create_dt TIMESTAMP NOT NULL
	,update_dt TIMESTAMP NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_fact_passport_blacklist PRIMARY KEY(passport_num)
	);

CREATE TABLE deaian.trsh_dwh_dim_terminals_hist(
	terminal_id VARCHAR(10) NOT NULL
	,terminal_type VARCHAR(10) NOT NULL
	,terminal_city VARCHAR(200) NOT NULL
	,terminal_address VARCHAR(255) NOT NULL
	,effective_from TIMESTAMP NOT NULL
	,effective_to TIMESTAMP NOT NULL DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD')
	,deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_dim_terminals_hist PRIMARY KEY(terminal_id, effective_from)
	);

CREATE TABLE deaian.trsh_dwh_dim_clients_hist(
	client_id VARCHAR(10) NOT NULL
	,last_name VARCHAR(20) NOT NULL
	,first_name VARCHAR(20) NOT NULL
	,patronymic VARCHAR(20) NULL
	,date_of_birth DATE NOT NULL
	,passport_num VARCHAR(15) NOT NULL
	,passport_valid_to DATE NULL
	,phone VARCHAR(16) NULL
	,effective_from TIMESTAMP NOT NULL
	,effective_to TIMESTAMP NOT NULL DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD')
	,deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_dim_clients_hist PRIMARY KEY(client_id, effective_from)
	);

CREATE TABLE deaian.trsh_dwh_dim_accounts_hist(
	account_num VARCHAR(20) NOT NULL
	,valid_to DATE NOT NULL
	,client VARCHAR(10) NOT NULL
	,effective_from TIMESTAMP NOT NULL
	,effective_to TIMESTAMP NOT NULL DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD')
	,deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_dim_accounts_hist PRIMARY KEY(account_num, effective_from)
	);

CREATE TABLE deaian.trsh_dwh_dim_cards_hist(
	card_num VARCHAR(19) NOT NULL
	,account_num VARCHAR(20) NOT NULL
	,effective_from TIMESTAMP NOT NULL
	,effective_to TIMESTAMP NOT NULL DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD')
	,deleted_flg BOOLEAN NOT NULL DEFAULT FALSE
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_dim_cards_hist PRIMARY KEY(card_num, effective_from)
	);

CREATE TABLE deaian.trsh_dwh_fact_transaction(
	trans_id VARCHAR(20) NOT NULL
	,trans_date TIMESTAMP NOT NULL
	,card_num VARCHAR(19) NOT NULL
	,oper_type VARCHAR(20) NOT NULL
	,amt DECIMAL(18,2) NOT NULL
	,oper_result VARCHAR(10) NOT NULL
	,terminal VARCHAR(10) NOT NULL
	,create_dt TIMESTAMP NOT NULL
	,update_dt TIMESTAMP NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_fact_transaction PRIMARY KEY(trans_id)
	);

CREATE TABLE deaian.trsh_dwh_dim_frauds(
	event_type INT NOT NULL
	,description VARCHAR(255) NOT NULL
	,create_dt TIMESTAMP NOT NULL
	,update_dt TIMESTAMP NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT pk_trsh_dwh_dim_frauds PRIMARY KEY(event_type)
	);

CREATE TABLE deaian.trsh_rep_fraud(
	event_dt TIMESTAMP NOT NULL
	,passport VARCHAR(15) NOT NULL
	,fio VARCHAR(62) NOT NULL
	,phone VARCHAR(16) NOT NULL
	,event_type INT NOT NULL
	,report_dt DATE NOT NULL
	,processed_dt TIMESTAMP NOT NULL
	,CONSTRAINT fk_trsh_rep_fraud FOREIGN KEY(event_type) REFERENCES deaian.trsh_dwh_dim_frauds(event_type)
	);


--------------------------------------------------
-- 3. Inserting data to tables
--------------------------------------------------
INSERT INTO deaian.trsh_dwh_dim_frauds(event_type, description, create_dt, processed_dt)
VALUES(	1
		,'Совершение операции при просроченном или заблокированном паспорте.'
		,TO_DATE('2023-10-06', 'YYYY-MM-DD')
		,NOW()
		);

INSERT INTO deaian.trsh_dwh_dim_frauds(event_type, description, create_dt, processed_dt)
VALUES(	2
		,'Совершение операции при недействующем договоре.'
		,TO_DATE('2023-10-06', 'YYYY-MM-DD')
		,NOW()
		);

INSERT INTO deaian.trsh_dwh_dim_frauds(event_type, description, create_dt, processed_dt)
VALUES(	3
		,'Совершение операций в разных городах в течение одного часа.'
		,TO_DATE('2023-10-06', 'YYYY-MM-DD')
		,NOW()
		);

INSERT INTO deaian.trsh_dwh_dim_frauds(event_type, description, create_dt, processed_dt)
VALUES(	4
		,'Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.'
		,TO_DATE('2023-10-06', 'YYYY-MM-DD')
		,NOW()
		);

INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_passport_blacklist', NOW());
INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_transactions', NOW());
INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_terminals', NOW());
INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_cards', NOW());
INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_accounts', NOW());
INSERT INTO deaian.trsh_meta_etl_update(schema_name, table_name, processed_dt) VALUES('deaian', 'trsh_stg_clients', NOW());


INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_fact_passport_blacklist'
		,ARRAY['passport_num', 'entry_dt']
		,ARRAY['passport_num']
		,1
		,'deaian'
		,'trsh_stg_passport_blacklist'
		,ARRAY['passport', 'date']
		,ARRAY['passport']
		,NOW()
		);

INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_fact_transaction'
		,ARRAY['trans_id', 'trans_date', 'amt', 'card_num', 'oper_type', 'oper_result', 'terminal']
		,ARRAY['trans_id']
		,1
		,'deaian'
		,'trsh_stg_transactions'
		,ARRAY['transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal']
		,ARRAY['transaction_id']
		,NOW()
		);
		
INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_dim_terminals_hist'
		,ARRAY['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address']
		,ARRAY['terminal_id']
		,2
		,'deaian'
		,'trsh_stg_terminals'
		,ARRAY['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address']
		,ARRAY['terminal_id']
		,NOW()
		);
		
INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_dim_cards_hist'
		,ARRAY['card_num', 'account_num']
		,ARRAY['card_num']
		,2
		,'deaian'
		,'trsh_stg_cards'
		,ARRAY['card_num', 'account']
		,ARRAY['card_num']
		,NOW()
		);
		
INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_dim_accounts_hist'
		,ARRAY['account_num', 'valid_to', 'client']
		,ARRAY['account_num']
		,2
		,'deaian'
		,'trsh_stg_accounts'
		,ARRAY['account', 'valid_to', 'client']
		,ARRAY['account']
		,NOW()
		);
		
INSERT INTO deaian.trsh_meta_core_table_mapping(target_schema_name, target_table_name, target_columns, target_keys, scd, source_schema_name, source_table_name, source_columns, source_keys, processed_dt)
VALUES(	'deaian'
		,'trsh_dwh_dim_clients_hist'
		,ARRAY['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone']
		,ARRAY['client_id']
		,2
		,'deaian'
		,'trsh_stg_clients'
		,ARRAY['client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone']
		,ARRAY['client_id']
		,NOW()
		);

#!/usr/bin/python3

from py_scripts import *


def main():
    db_tgt = Database(host='de-edu-db.chronosavant.ru', port=5432, database='edu', user='deaian',
                      password='sarumanthewhite')
    files_src = FileFinder(path='.', templates=('passport_blacklist_*.xlsx', 'terminals_*.xlsx', 'transactions_*.txt'))
    db_src = Database(host='de-edu-db.chronosavant.ru', port=5432, database='bank', user='bank_etl',
                      password='bank_etl_password')
    db_tables = ('accounts', 'clients', 'cards')

    etl = ETL(db_tgt)
    etl.from_file(files_src)
    etl.from_database(db_src, db_tables)
    etl.mart_update()
    etl.save()


if __name__ == '__main__':
    main()

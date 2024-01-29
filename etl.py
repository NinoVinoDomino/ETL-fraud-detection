from typing import Iterable, Tuple, List, Union
from datetime import datetime
from .database import Database
from .file import File


class ETL:
    """Class for performing ETL processes"""

    def __init__(self, target: Database) -> None:
        self.__target = target
        self.__run_start_dt = datetime.now()
        self.__meta = self.__get_meta_etl_update()
        self.__mapping = self.__get_meta_core_table_mapping()
        self.__run_id = self.__get_run_id()

    def __get_meta_etl_update(self) -> List[tuple]:
        """Method for getting tables last update dates"""
        query = '''
                SELECT      schema_name
                            ,table_name
                            ,max_update_dt
                FROM        deaian.trsh_meta_etl_update;
                '''
        _, data = self.__target.select(query)
        return data

    def __get_meta_core_table_mapping(self) -> List[tuple]:
        """Method for getting core tables mapping"""
        query = '''
                SELECT      target_schema_name
                            ,target_table_name
                            ,target_columns
                            ,target_keys
                            ,scd
                            ,source_schema_name
                            ,source_table_name
                            ,source_columns
                            ,source_keys
                FROM        deaian.trsh_meta_core_table_mapping;
                '''
        _, data = self.__target.select(query)
        return data

    def __get_run_id(self) -> int:
        """Method for getting run_id"""
        query = '''
                SELECT NEXTVAL('deaian.trsh_etl_run');
                '''
        _, data = self.__target.select(query)
        return data[0][0]

    @staticmethod
    def __columns_to_string(columns: Union[Tuple[str, ...], List[str]], mode: int = 1, alias: str = None) -> str:
        """Method for converting file column names to string"""
        if alias is not None:
            columns = tuple(f'{alias}.{column}' for column in columns)
        if mode == 1:
            return ', '.join(columns + ('create_dt', 'processed_dt'))
        elif mode == 0:
            return ', '.join(columns + ('processed_dt',))
        else:
            return ', '.join(columns)

    @staticmethod
    def __generate_values(num: int) -> str:
        """Method for generating values string"""
        return ', '.join('%s' for _ in range(num))

    @staticmethod
    def __generate_table_name(table: str, prefix: str = None, schema: str = None) -> str:
        """Method for extending table name"""
        return (f'{schema}.' if schema else '') + (f'{prefix}_' if prefix else '') + table

    def __get_last_update_dt(self, table: str, schema: str) -> datetime:
        """Method for comparing file date vs last update date"""
        for row in self.__meta:
            if table == row[1] and schema == row[0]:
                return row[2]

    def __clean_stg(self, full_table_name) -> int:
        """Method for cleaning stg tables before loading"""
        query = f'''
                DELETE FROM {full_table_name};
                '''
        return self.__target.execute(query)

    def __insert_file_to_stg(self, file: File, prefix: str, schema: str) -> int:
        """Method for downloading data from file to stg"""
        query = f'''
                INSERT INTO {self.__generate_table_name(table=file.name, prefix=prefix, schema=schema)}({self.__columns_to_string(file.headers)})
                VALUES ({self.__generate_values(len(file.headers))}, TO_DATE('{file.dt}', 'YYYY-MM-DD'), NOW());
                '''
        return self.__target.insert(query, file.data)

    def __set_new_update_dt(self, table: str, schema: str) -> None:
        """Method for updating max update date in meta table"""
        query = f'''
                UPDATE		deaian.trsh_meta_etl_update
                SET			max_update_dt = (SELECT (MAX(create_dt)) FROM {schema}.{table})
                            ,processed_dt = NOW()
                WHERE		schema_name = '{schema}'
                            AND table_name = '{table}'
                            AND max_update_dt < (SELECT (MAX(create_dt)) FROM {schema}.{table});
                '''
        self.__target.execute(query)

    def __save_etl_run_log(self, schema, table, deleted: int = 0, updated: int = 0, inserted: int = 0) -> None:
        """Method for saving ETL log to meta table"""
        query = f'''
                INSERT INTO deaian.trsh_meta_etl_run_log(run_id, schema_name, table_name, rows_deleted,
                                                        rows_updated, rows_inserted, run_start_dt, processed_dt)
                VALUES({self.__generate_values(7)}, NOW());
                '''
        data = (self.__run_id, schema, table, deleted, updated, inserted, self.__run_start_dt)
        self.__target.insert(query, [data])

    def __save_etl_run_log_end_dt(self) -> None:
        """Method for saving ETL finish date to meta table"""
        query = f'''
                UPDATE      deaian.trsh_meta_etl_run_log
                SET         run_end_dt = '{datetime.now()}'
                WHERE       run_id = {self.__run_id};
                '''
        self.__target.execute(query)

    def save(self) -> None:
        """Method for saving ETL finish date to meta table for all tables"""
        self.__save_etl_run_log_end_dt()
        self.__target.save()

    def from_file(self, files: Iterable[str], schema: str = 'deaian', prefix: str = 'trsh_stg') -> None:
        """Method for processing ETL loading from file"""
        for filepath in files:
            file = File(filepath)
            stg_full_table_name = self.__generate_table_name(table=file.name, prefix=prefix, schema=schema)
            short_table_name = self.__generate_table_name(table=file.name, prefix=prefix)
            mapping = self.__get_mapping(table=file.name, prefix=prefix, schema=schema)
            stg_columns = tuple(mapping.get('source_columns'))
            stg_keys = tuple(mapping.get('source_keys'))
            dwh_schema = mapping.get('target_schema_name')
            dwh_table = mapping.get('target_table_name')
            dwh_table_name = self.__generate_table_name(table=dwh_table, schema=dwh_schema)
            dwh_keys = mapping.get('target_keys')
            dwh_columns = mapping.get('target_columns')
            scd = mapping.get('scd')

            # Loading data to STG
            deleted = self.__clean_stg(stg_full_table_name)
            if self.__get_last_update_dt(table=short_table_name, schema=schema) < file.dt:
                inserted = self.__insert_file_to_stg(file=file, prefix=prefix, schema=schema)
                self.__set_new_update_dt(table=short_table_name, schema=schema)
            else:
                inserted = 0
            self.__save_etl_run_log(schema, short_table_name, deleted=deleted, inserted=inserted)
            self.__target.save()

            # Loading data to DWH
            if scd == 1:
                dwh_updated = self.__scd1_updating(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                   stg_keys=stg_keys, dwh_table_name=dwh_table_name,
                                                   dwh_columns=dwh_columns, dwh_keys=dwh_keys)
                dwh_deleted = 0
            elif scd == 2:
                dwh_deleted = self.__scd2_deleting(stg_del_table_name=stg_full_table_name, stg_keys=stg_keys,
                                                   dwh_table_name=dwh_table_name, dwh_columns=dwh_columns,
                                                   dwh_keys=dwh_keys)
                dwh_updated = self.__scd2_updating(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                   stg_keys=stg_keys, dwh_table_name=dwh_table_name,
                                                   dwh_columns=dwh_columns, dwh_keys=dwh_keys)
            else:
                dwh_deleted = 0
                dwh_updated = 0

            dwh_inserted = self.__scd_inserting(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                stg_keys=stg_keys, dwh_table_name=dwh_table_name
                                                , dwh_columns=dwh_columns, dwh_keys=dwh_keys, scd=scd)
            self.__save_etl_run_log(schema=dwh_schema, table=dwh_table, deleted=dwh_deleted, updated=dwh_updated,
                                    inserted=dwh_inserted)
            self.__target.save()

    def __get_mapping(self, table: str, prefix: str, schema: str) -> dict:
        """Method for getting column names from table"""
        headers = ['target_schema_name', 'target_table_name', 'target_columns', 'target_keys', 'scd',
                   'source_schema_name', 'source_table_name', 'source_columns', 'source_keys']
        for row in self.__mapping:
            if row[5] == schema and row[6] == self.__generate_table_name(table=table, prefix=prefix):
                return dict(zip(headers, row))

    def __get_data(self, db: Database, table_name: str, columns: tuple, last_update_dt: datetime) -> List[tuple]:
        """Method for getting data from source database"""
        query = f'''
                SELECT      {self.__columns_to_string(columns=columns, mode=2)}
                            ,COALESCE(update_dt, create_dt) AS create_dt
                FROM        {table_name}
                WHERE       COALESCE(update_dt, create_dt) > TO_DATE('{last_update_dt}', 'YYYY-MM-DD');
                '''
        _, data = db.select(query=query)
        return data

    def __get_indices(self, db: Database, table_name: str, columns: list) -> List[tuple]:
        """Method for getting table indices for checking deletion"""
        query = f'''
                SELECT      {self.__columns_to_string(columns=columns, mode=2)}
                FROM        {table_name};
                '''
        _, data = db.select(query=query)
        return data

    def __insert_data_to_stg(self, table: str, schema: str, prefix: str, columns: tuple, data: List[tuple],
                             add_col=1) -> int:
        """Method for inserting data to stg"""
        query = f'''
                INSERT INTO {self.__generate_table_name(table, prefix, schema)}({self.__columns_to_string(columns, mode=add_col)})
                VALUES ({self.__generate_values(len(columns) + add_col)}, NOW());
                '''
        return self.__target.insert(query, data)

    def from_database(self, db: Database, tables: Tuple[str, ...], source_schema: str = 'info',
                      target_schema: str = 'deaian', prefix: str = 'trsh_stg') -> None:
        """Method for processing ETL loading from database"""
        for table in tables:
            mapping = self.__get_mapping(table=table, prefix=prefix, schema=target_schema)
            stg_columns = tuple(mapping.get('source_columns'))
            stg_keys = tuple(mapping.get('source_keys'))
            dwh_schema = mapping.get('target_schema_name')
            dwh_table = mapping.get('target_table_name')
            dwh_table_name = self.__generate_table_name(table=dwh_table, schema=dwh_schema)
            dwh_keys = mapping.get('target_keys')
            dwh_columns = mapping.get('target_columns')
            scd = mapping.get('scd')

            # Loading data to STG
            stg_full_table_name = self.__generate_table_name(table=table, prefix=prefix, schema=target_schema)
            stg_short_table_name = self.__generate_table_name(table=table, prefix=prefix)
            source_table_name = self.__generate_table_name(table=table, schema=source_schema)
            last_update_dt = self.__get_last_update_dt(table=stg_short_table_name, schema=target_schema)
            source_data = self.__get_data(db=db, table_name=source_table_name, columns=stg_columns,
                                          last_update_dt=last_update_dt)
            stg_deleted = self.__clean_stg(stg_full_table_name)
            stg_inserted = self.__insert_data_to_stg(table=table, schema=target_schema, prefix=prefix,
                                                     columns=stg_columns, data=source_data)
            self.__set_new_update_dt(table=stg_short_table_name, schema=target_schema)
            self.__save_etl_run_log(schema=target_schema, table=stg_short_table_name, deleted=stg_deleted,
                                    inserted=stg_inserted)
            self.__target.save()

            # Loading ids to STG
            stg_del_table_name = f'{table}_del'
            stg_del_full_table_name = f'{stg_full_table_name}_del'
            stg_del_short_table_name = f'{stg_short_table_name}_del'
            stg_del_deleted = self.__clean_stg(stg_del_full_table_name)
            source_del_data = self.__get_indices(db=db, table_name=source_table_name,
                                                 columns=mapping.get('source_keys'))
            stg_del_inserted = self.__insert_data_to_stg(table=stg_del_table_name, schema=target_schema, prefix=prefix,
                                                         columns=stg_keys, data=source_del_data, add_col=0)
            self.__save_etl_run_log(schema=target_schema, table=stg_del_short_table_name,
                                    deleted=stg_del_deleted,
                                    inserted=stg_del_inserted)
            self.__target.save()

            # Loading data to DWH
            if scd == 1:
                dwh_updated = self.__scd1_updating(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                   stg_keys=stg_keys,
                                                   dwh_table_name=dwh_table_name, dwh_columns=dwh_columns,
                                                   dwh_keys=dwh_keys)
                dwh_deleted = 0
            elif scd == 2:
                dwh_deleted = self.__scd2_deleting(stg_del_table_name=stg_del_full_table_name, stg_keys=stg_keys,
                                                   dwh_table_name=dwh_table_name, dwh_columns=dwh_columns,
                                                   dwh_keys=dwh_keys)
                dwh_updated = self.__scd2_updating(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                   stg_keys=stg_keys, dwh_table_name=dwh_table_name,
                                                   dwh_columns=dwh_columns, dwh_keys=dwh_keys)
            else:
                dwh_deleted = 0
                dwh_updated = 0
            dwh_inserted = self.__scd_inserting(stg_table_name=stg_full_table_name, stg_columns=stg_columns,
                                                stg_keys=stg_keys, dwh_table_name=dwh_table_name,
                                                dwh_columns=dwh_columns, dwh_keys=dwh_keys, scd=scd)
            self.__save_etl_run_log(schema=dwh_schema, table=dwh_table, deleted=dwh_deleted, updated=dwh_updated,
                                    inserted=dwh_inserted)
            self.__target.save()

    @staticmethod
    def __matching(stg_table_name: str, stg_keys: Union[Tuple[str, ...], List[str]]
                   , dwh_table_name: Union[str, None], dwh_keys: Union[Tuple[str, ...], List[str]]) -> str:
        """Method for matching keys or columns between STG and DWH"""
        if dwh_table_name is None:
            return '\n,'.join(
                [f'{dwh} = {stg_table_name}.{stg}' for stg, dwh in zip(stg_keys, dwh_keys)])
        else:
            return '\nAND '.join(
                [f'{dwh_table_name}.{dwh} = {stg_table_name}.{stg}' for stg, dwh in zip(stg_keys, dwh_keys)])

    def __scd2_deleting(self, stg_del_table_name: str, stg_keys: Union[Tuple[str, ...], List[str]], dwh_table_name: str,
                        dwh_columns: Union[Tuple[str, ...], List[str]],
                        dwh_keys: Union[Tuple[str, ...], List[str]]) -> int:
        """Method for performing SCD2 deleting"""
        query = f'''
                UPDATE		{dwh_table_name}
                SET			effective_to = CURRENT_DATE - INTERVAL '1 SECOND'
                            ,processed_dt = NOW()
                FROM		{dwh_table_name} AS dwh
                WHERE		{self.__matching(stg_table_name='dwh', stg_keys=dwh_keys,
                                              dwh_table_name=dwh_table_name, dwh_keys=dwh_keys)}
                            AND {dwh_table_name}.effective_to = dwh.effective_to
                            AND dwh.effective_to = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = FALSE
                            AND NOT EXISTS(	SELECT		1
                                            FROM		{stg_del_table_name} AS del
                                            WHERE		{self.__matching(stg_table_name='del', stg_keys=stg_keys,
                                                                          dwh_table_name='dwh', dwh_keys=dwh_keys)});
                                                                             
                INSERT INTO {dwh_table_name}({self.__columns_to_string(dwh_columns, mode=3)}, effective_from, deleted_flg, processed_dt)
                SELECT		{self.__columns_to_string(dwh_columns, mode=3)}
                            ,CURRENT_DATE
                            ,TRUE
                            ,NOW()
                FROM		{dwh_table_name} AS dwh
                WHERE		dwh.effective_to = (SELECT      MAX(effective_to)
                                                FROM        {dwh_table_name} AS et
                                                WHERE       {self.__matching(stg_table_name='et', stg_keys=dwh_keys,
                                                                             dwh_table_name='dwh', dwh_keys=dwh_keys)})
                            AND dwh.deleted_flg = FALSE
                            AND NOT EXISTS(	SELECT		1
                                            FROM		{stg_del_table_name} AS del
                                            WHERE		{self.__matching(stg_table_name='del', stg_keys=stg_keys,
                                                                          dwh_table_name='dwh', dwh_keys=dwh_keys)});
                '''
        return self.__target.execute(query)

    def __scd2_updating(self, stg_table_name: str, stg_columns: Union[Tuple[str, ...], List[str]],
                        stg_keys: Union[Tuple[str, ...], List[str]],
                        dwh_table_name: str, dwh_columns: Union[Tuple[str, ...], List[str]],
                        dwh_keys: Union[Tuple[str, ...], List[str]]) -> int:
        """Method for performing SCD2 updating"""
        query = f'''
                UPDATE		{dwh_table_name}
                SET			effective_to = stg.create_dt - INTERVAL '1 SECOND'
                            ,processed_dt = NOW()
                FROM		{dwh_table_name} AS dwh
                            INNER JOIN {stg_table_name} AS stg ON {self.__matching(stg_table_name='stg', stg_keys=stg_keys,
                                                                                   dwh_table_name='dwh', dwh_keys=dwh_keys)}
                WHERE		{self.__matching(stg_table_name='dwh', stg_keys=dwh_keys,
                                              dwh_table_name=dwh_table_name, dwh_keys=dwh_keys)}
                            AND {dwh_table_name}.effective_to = dwh.effective_to
                            AND dwh.effective_to = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                            AND NOT EXISTS(	SELECT		{self.__columns_to_string(dwh_columns, mode=3, alias='dwh')}, dwh.deleted_flg
                                            INTERSECT
                                            SELECT		{self.__columns_to_string(stg_columns, mode=3, alias='stg')}, FALSE);
                                            
                INSERT INTO {dwh_table_name}({self.__columns_to_string(dwh_columns, mode=3)}, effective_from, deleted_flg, processed_dt)
                SELECT		{self.__columns_to_string(stg_columns, mode=3, alias='stg')}
                            ,stg.create_dt
                            ,FALSE
                            ,NOW()
                FROM		{dwh_table_name} AS dwh
                            INNER JOIN {stg_table_name} AS stg ON {self.__matching(stg_table_name='stg', stg_keys=stg_keys,
                                                                                   dwh_table_name='dwh', dwh_keys=dwh_keys)}
                WHERE		dwh.effective_to = (SELECT      MAX(effective_to)
                                                FROM        {dwh_table_name} AS et
                                                WHERE       {self.__matching(stg_table_name='et', stg_keys=dwh_keys,
                                                                             dwh_table_name='dwh', dwh_keys=dwh_keys)})
                            AND NOT EXISTS(	SELECT		{self.__columns_to_string(dwh_columns, mode=3, alias='dwh')}, dwh.deleted_flg
                                            INTERSECT
                                            SELECT		{self.__columns_to_string(stg_columns, mode=3, alias='stg')}, FALSE);
                '''
        return self.__target.execute(query)

    def __scd_inserting(self, stg_table_name: str, stg_columns: Union[Tuple[str, ...], List[str]],
                        stg_keys: Union[Tuple[str, ...], List[str]],
                        dwh_table_name: str, dwh_columns: Union[Tuple[str, ...], List[str]],
                        dwh_keys: Union[Tuple[str, ...], List[str]], scd: int) -> int:
        """Method for performing SCD inserting"""
        query = f'''
                INSERT INTO {dwh_table_name}({self.__columns_to_string(dwh_columns, mode=3)}, {'effective_from' if scd == 2 else 'create_dt'}, processed_dt)
                SELECT		{self.__columns_to_string(stg_columns, mode=3)}
                            ,create_dt
                            ,NOW()
                FROM		{stg_table_name} AS stg
                WHERE		NOT EXISTS(	SELECT		1
                                        FROM		{dwh_table_name} AS dwh
                                        WHERE		{self.__matching(stg_table_name='stg', stg_keys=stg_keys,
                                                                      dwh_table_name='dwh', dwh_keys=dwh_keys)});
                '''
        return self.__target.execute(query)

    def __scd1_updating(self, stg_table_name: str, stg_columns: Union[Tuple[str, ...], List[str]],
                        stg_keys: Union[Tuple[str, ...], List[str]],
                        dwh_table_name: Union[str, None], dwh_columns: Union[Tuple[str, ...], List[str]],
                        dwh_keys: Union[Tuple[str, ...], List[str]]) -> int:
        """Method for performing SCD1 updating"""
        query = f'''
                UPDATE		{dwh_table_name}
                SET			{self.__matching(stg_table_name='stg', stg_keys=stg_columns, dwh_table_name=None, dwh_keys=dwh_columns)}
                            ,update_dt = stg.create_dt
                            ,processed_dt = NOW()
                FROM		{dwh_table_name} AS dwh
                            INNER JOIN {stg_table_name} AS stg ON {self.__matching(stg_table_name='stg', stg_keys=stg_keys,
                                                                                   dwh_table_name='dwh', dwh_keys=dwh_keys)}
                WHERE		NOT EXISTS(	SELECT		{self.__columns_to_string(dwh_columns, mode=3, alias='dwh')}
                                        INTERSECT
                                        SELECT		{self.__columns_to_string(stg_columns, mode=3, alias='stg')});
                '''
        return self.__target.execute(query)

    def mart_update(self) -> None:
        """Method for generating report"""
        query = self.__target.get_script('./sql_scripts/trsh_rep_fraud_sync.sql')
        rep_inserted = self.__target.execute(query)
        self.__save_etl_run_log(schema='deaian', table='trsh_rep_fraud', inserted=rep_inserted)
        self.__target.save()


if __name__ == '__main__':
    pass

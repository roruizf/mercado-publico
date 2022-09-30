from dotenv import dotenv_values
import os
import pandas as pd
import psycopg2
# import psycopg2.extras as extras
import numpy as np


def load():
    data_df = _read_data_files()
    data_df = _set_columns_types(data_df)
    conn = _connect_to_database()
    table_name = 'tender_index_list'
    _create_table(conn, table_name)
    stored_data_df = _get_stored_index_list(conn)
    data_to_insert_df, data_to_upsert_df = _compare_stored_and_new_data(
        data_df, stored_data_df)
    _load_data_into_database(conn, data_to_insert_df, 'tender_index_list')
    _load_data_into_database(conn, data_to_upsert_df, 'tender_index_list')
    # _insert_values(conn, data_to_insert_df, 'tender_index_list')
    # _insert_values(conn, data_to_upsert_df, 'tender_index_list')
    # print('stored data')
    # print(stored_data_df.tail())
    # print('data to add')
    # print(data_to_insert_df['estado'].head())
    # print(data_to_upsert_df.empty)


def _read_data_files():
    dataset_path = "./data/interim/"
    files = os.listdir(dataset_path)
    # Looking for 'data-tender_list'
    data_files = pd.Series(files)
    data_files_list = data_files[data_files.str.startswith(
        'data-tender-index_list-from')].to_list()
    # Create a dataframe contaning from files in data_files_list
    data_df = pd.DataFrame([], columns=[
                           'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCierre', 'FechaPublicacion'])
    for file in data_files_list:
        csv_file_name = file
        csv_file_path = os.path.join(dataset_path, csv_file_name)
        data_df_i = pd.read_csv(csv_file_path, parse_dates=[
                                'FechaCierre', 'FechaPublicacion'], infer_datetime_format=True, encoding='utf-8-sig')
        data_df = pd.concat([data_df, data_df_i],
                            ignore_index=True, sort=False)
    return data_df


def _set_columns_types(data_df):
    data_df['CodigoExterno'] = data_df['CodigoExterno'].astype(str)
    data_df['Nombre'] = data_df['Nombre'].astype(str)
    data_df['CodigoEstado'] = data_df['CodigoEstado'].astype(int)
    data_df['Estado'] = data_df['Estado'].astype(str)
    data_df['FechaCierre'] = pd.to_datetime(
        data_df['FechaCierre'], format='%Y-%m-%d %H:%M:%S')
    data_df['FechaPublicacion'] = pd.to_datetime(
        data_df['FechaPublicacion'], format='%Y-%m-%d')
    data_df = data_df[['CodigoExterno', 'Nombre', 'CodigoEstado', 'Estado', 'FechaCierre',
                       'FechaPublicacion']]
    return data_df


def _connect_to_database():
    config_credentials = dict(dotenv_values("../database_credentials.env"))
    host = config_credentials['host']
    port = config_credentials['port']
    database = config_credentials['database']
    user = config_credentials['user']
    password = config_credentials['password']

    # CONNECTION TO THE DATABASE
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        print("Successful connection")
        #cursor = conn.cursor()
        #row = cursor.fetchone()
        # print(row)

    except Exception as ex:
        print(ex)
    return conn


def _create_table(conn, table_name):
    # columns: codigo_externo, nombre, codigo_estado, estado, fecha_cierre, fecha_publicacion

    query = f"""
            CREATE TABLE IF NOT EXISTS public."{table_name}"
            (
                id SERIAL NOT NULL,
                codigo_externo character varying(20) NOT NULL UNIQUE,
                nombre character varying(255),
                codigo_estado integer, 
                estado character varying(12),
                fecha_cierre timestamp without time zone,
                fecha_publicacion date,                                                    
                CONSTRAINT "{table_name}_pkey" PRIMARY KEY (codigo_externo)
            )
            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public."{table_name}"
                OWNER to postgres;
            """

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


def _get_stored_index_list(conn):
    sql_query = """
                SELECT *
	            FROM public.tender_index_list;
                """
    stored_data_df = pd.io.sql.read_sql_query(sql_query, conn)
    return stored_data_df


def _compare_stored_and_new_data(data_df, stored_data_df):
    data_df.columns = ['codigo_externo',
                       'nombre',
                       'codigo_estado',
                       'estado',
                       'fecha_cierre',
                       'fecha_publicacion']
    # print(data_df.columns)
    # print(stored_data_df.columns)
    # Getting common 'codigo_externo'
    common_codigo_externo = list(set(data_df['codigo_externo'].to_list()) & set(
        stored_data_df['codigo_externo'].to_list()))
    # Distinct 'codigo_externo' rows -> this is new data
    data_to_insert_df = data_df[~data_df['codigo_externo'].isin(
        common_codigo_externo)].reset_index(drop=True)

    # Mutual 'codigo_externo' rows
    mutual_data_df = data_df[data_df['codigo_externo'].isin(
        common_codigo_externo)].sort_values(by='codigo_externo').reset_index(drop=True)
    mutual_stored_data_df = stored_data_df[stored_data_df['codigo_externo'].isin(
        common_codigo_externo)].sort_values(by='codigo_externo').reset_index(drop=True)
    common_codigo_estado = (
        mutual_data_df['codigo_estado'] == mutual_stored_data_df['codigo_estado'])

    # Distinct 'codigo_estado' rows -> these data exist but 'codigo_estado' have changed
    data_to_upsert_df = mutual_data_df[~common_codigo_estado]

    return data_to_insert_df, data_to_upsert_df


def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


def _load_data_into_database(conn, df, table_name):

    # Inserting each row
    for index, row in df.iterrows():
        query = """
        INSERT INTO public.%s(
	    codigo_externo, nombre, codigo_estado, estado, fecha_cierre, fecha_publicacion)
	    VALUES ('%s', '%s', %s, '%s', '%s', '%s')
        ON CONFLICT (codigo_externo) DO UPDATE
        SET nombre = EXCLUDED.nombre, codigo_estado = EXCLUDED.codigo_estado, estado=EXCLUDED.estado,
        fecha_cierre = EXCLUDED.fecha_cierre, fecha_publicacion = EXCLUDED.fecha_publicacion;
        """ % (table_name, row['codigo_externo'], row['nombre'], row['codigo_estado'], row['estado'],
               row['fecha_cierre'].to_pydatetime(), row['fecha_publicacion'].to_pydatetime())
        # print(query)
        single_insert(conn, query)


if __name__ == '__main__':
    load()

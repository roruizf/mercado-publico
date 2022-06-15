import pandas as pd
import os


def transform():
    data_df = _read_data_files()
    data_df = _drop_na(data_df)
    data_df = _drop_duplicates(data_df)
    data_df = _normalize_column(data_df, 'Nombre')
    data_df = _add_column_estado(data_df)
    info_df = _read_info_files()
    _save_transformed_data(info_df, data_df)
    # print(info_df.head())
    # print(data_df.head())


def _read_data_files():
    dataset_path = "./data/raw/"
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


def _drop_na(data_df):
    # Drop NA and reset index
    print(f'Dropping NA values...')
    rows_before_dropping = data_df.shape[0]
    print(f' - Number of rows BEFORE dropping NA: {rows_before_dropping}')
    data_df = data_df.dropna().reset_index(drop=True)
    rows_after_dropping = data_df.shape[0]
    print(f' - Number of rows AFTER dropping NA: {rows_after_dropping}')
    print(
        f' - Number of rows DROPPED: {rows_before_dropping-rows_after_dropping}')
    return data_df


def _drop_duplicates(data_df):
    print(f'Dropping DUPLICATED values...')
    # Drop duplicates
    # Sort by=['FechaPublicacion', 'FechaCierre']
    data_df['CodigoEstado'] = data_df['CodigoEstado'].astype(int)
    data_df = data_df.sort_values(
        by=['FechaPublicacion', 'FechaCierre', 'CodigoEstado']).reset_index(drop=True)
    rows_before_dropping = data_df.shape[0]
    print(
        f' - Number of rows BEFORE dropping duplicates: {rows_before_dropping}')
    data_df = data_df.drop_duplicates(
        subset=['CodigoExterno'], keep='last').reset_index(drop=True)
    rows_after_dropping = data_df.shape[0]
    print(
        f' - Number of rows AFTER dropping duplicates: {rows_after_dropping}')
    print(
        f' - Number of rows DROPPED: {rows_before_dropping-rows_after_dropping}')
    return data_df


def _normalize_column(data_df, column):
    print(f'Normalizing column: {column}')
    data_df[column] = data_df[column].str.normalize('NFKD').str.encode(
        'ascii', errors='ignore').str.decode('utf-8').str.capitalize().str.replace("'", '')
    return data_df


def _add_column_estado(data_df):
    print(f'Adding column: Estado')
    # Adding column estado
    data_df['CodigoEstado'] = data_df['CodigoEstado'].astype(int)
    codigo_estado_dict = {5: "Publicada",
                          6: "Cerrada",
                          7: "Desierta",
                          8: "Adjudicada",
                          15: "Revocada",
                          16: "Suspendida"}
    data_df['Estado'] = data_df['CodigoEstado'].map(codigo_estado_dict)
    data_df = data_df[['CodigoExterno', 'Nombre', 'CodigoEstado',
                       'Estado', 'FechaCierre', 'FechaPublicacion']]
    return data_df


def remove_existing_files(destination_directory_path='./data/raw/'):
    # check destination_directory_path is empty
    files_in_directory = os.listdir(destination_directory_path)
    if len(files_in_directory) != 0:  # -> directory is NOT empty then delete those files
        # print(files_in_directory)
        for file in files_in_directory:
            os.remove(os.path.join(destination_directory_path, file))
            print(f'- File {file} deleted!')


def _save_transformed_data(info_df, data_df):
    initial_date = data_df['FechaPublicacion'].dt.strftime('%Y-%m-%d').iloc[0]
    end_date = data_df['FechaPublicacion'].dt.strftime('%Y-%m-%d').iloc[-1]
    data_df['FechaPublicacion'] = data_df['FechaPublicacion'].dt.strftime(
        '%Y-%m-%d')
    # Remove previous saved files if they exist...
    remove_existing_files()

    # Save data...
    print(f"Saving TRANSFORMED data from {initial_date} to {end_date}")

    data_df_csv_name = f"./data/interim/data-tender-index_list-from-{initial_date}-to-{end_date}.csv"
    data_df.to_csv(data_df_csv_name, sep=',',
                   index=False, encoding='utf-8')
    info_df_csv_name = f"./data/interim/info-tender-index_list-from-{initial_date}-to-{end_date}.csv"
    info_df.to_csv(info_df_csv_name, sep=',',
                   index=False, encoding='utf-8')


def remove_existing_files(destination_directory_path='./data/interim/'):
    # check destination_directory_path is empty
    files_in_directory = os.listdir(destination_directory_path)
    print(f"\nRemoving EXISTING files...")
    if len(files_in_directory) != 0:  # -> directory is NOT empty then delete those files
        # print(files_in_directory)
        for file in files_in_directory:
            os.remove(os.path.join(destination_directory_path, file))
            print(f'- File {file} deleted!')


def _read_info_files():
    dataset_path = "./data/raw/"
    files = os.listdir(dataset_path)
    # Looking for 'info-tender_list'
    info_files = pd.Series(files)
    info_files_list = info_files[info_files.str.startswith(
        'info-tender-index_list-from')].to_list()
    # Create a dataframe contaning from files in info_files_list
    info_df = pd.DataFrame([], columns=['Cantidad', 'FechaCreacion',
                           'Version', 'FechaPublicacion', 'ResponseStatusCode', 'NumberOrders'])
    for file in info_files_list:
        csv_file_name = file
        csv_file_path = os.path.join(dataset_path, csv_file_name)
        info_df_i = pd.read_csv(csv_file_path, parse_dates=[
                                'FechaPublicacion'], infer_datetime_format=True, encoding='utf-8-sig')
        info_df = pd.concat([info_df, info_df_i],
                            ignore_index=True, sort=False)
    return info_df


if __name__ == '__main__':
    transform()

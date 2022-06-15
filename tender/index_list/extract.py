import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time


def extract(initial_date, end_date):
    start_time = time.time()
    info_df, data_df = download_tender_list_for_a_giving_period(
        initial_date, end_date)
    save_downloaded_data(info_df, data_df)
    elapsed_time = time.time() - start_time
    print("\nElapsed time: %0.2f seconds." % elapsed_time)


def download_tender_list_for_a_giving_period(initial_date, end_date):
    # initial_date = '01-01-2022'
    # end_date = '28-02-2022'
    initial_datetime = datetime.strptime(initial_date, '%d-%m-%Y')
    end_datetime = datetime.strptime(end_date, '%d-%m-%Y')
    date_list = pd.date_range(
        initial_datetime, end_datetime, freq='d').strftime('%d%m%Y').tolist()
    print(f"Downloading data from {initial_date} to {end_date} (included)")
    data_df = pd.DataFrame([], columns=[
                           'CodigoExterno', 'Nombre', 'CodigoEstado', 'FechaCierre', 'FechaPublicacion'])
    info_df = pd.DataFrame([], columns=['Cantidad', 'FechaCreacion',
                           'Version', 'FechaPublicacion', 'ResponseStatusCode', 'NumberOrders'])
    for date_i in date_list:
        info_df_i, data_df_i = download_tender_list_for_a_giving_date(date_i)
        data_df = pd.concat([data_df, data_df_i],
                            ignore_index=True, sort=False)
        info_df = pd.concat([info_df, info_df_i],
                            ignore_index=True, sort=False)
        time.sleep(1)
    return info_df, data_df


def download_tender_list_for_a_giving_date(date):
    ticket = 'F8537A18-6766-4DEF-9E59-426B4FEE2844'
    url = 'http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha=' + \
        date + '&ticket=' + ticket
    # url = 'http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?fecha=' + date + '&CodigoOrganismo=' + '6938' + '&ticket=' + ticket

    # Downloading data for a given day -> making sure the data is downladed (status_code == 200)
    status = True
    try_number = 1
    sleep_time = 1  # s
    while status:
        response = requests.get(url)
        if response.status_code != 200:
            if try_number <= 10:
                print(
                    f"Status code is {response.status_code} at try number {try_number}, entering sleep for {sleep_time} second(s)")
                try_number += 1
                time.sleep(sleep_time)
            else:
                print(
                    f"Oops!  Maximum number of tries was reached ({try_number}).  Run the code again...")
                break
        else:
            print(
                f"Data downloaded for {date[0:2]}-{date[2:4]}-{date[4:]} (status_code=200)")
            status = False

    # Dictionary containing 4 keys: ['Cantidad', 'FechaCreacion', 'Version', 'Listado']
    data = response.json()

    # Dataframe coontaining tender list for a given day
    # if Cantidad = 0, then Listado =[] and data_df = empty
    data_df = pd.DataFrame.from_dict(data['Listado'])

    if data_df.empty:
        data_df.loc[0, 'CodigoExterno'] = np.nan
        data_df.loc[0, 'Nombre'] = np.nan
        data_df.loc[0, 'CodigoEstado'] = np.nan
        data_df.loc[0, 'FechaCierre'] = np.nan

    data_df['FechaPublicacion'] = date[4:] + '-' + date[2:4] + '-' + date[0:2]

    # Dataframe containing summary of downloaded data for a given day
    info_df = pd.DataFrame(columns=['Cantidad', 'FechaCreacion', 'Version',
                           'FechaPublicacion', 'ResponseStatusCode', 'NumberOrders'])
    info_df.loc[0, 'Cantidad'] = data['Cantidad']
    info_df.loc[0, 'FechaCreacion'] = data['FechaCreacion']
    info_df.loc[0, 'Version'] = data['Version']
    info_df.loc[0, 'FechaPublicacion'] = date[4:] + \
        '-' + date[2:4] + '-' + date[0:2]
    info_df.loc[0, 'ResponseStatusCode'] = response.status_code
    info_df.loc[0, 'NumberOrders'] = data_df.dropna().shape[0]

    return info_df, data_df


def save_downloaded_data(info_df, data_df):
    initial_date = data_df['FechaPublicacion'].sort_values(
        ascending=True).iloc[0]
    end_date = data_df['FechaPublicacion'].sort_values(ascending=True).iloc[-1]
    print(f"Saving data from {initial_date} to {end_date}")
    info_df_csv_name = f"./data/raw/info-tender-index_list-from-{initial_date}-to-{end_date}.csv"
    info_df.to_csv(info_df_csv_name, sep=',',
                   index=False, encoding='utf-8-sig')
    data_df_csv_name = f"./data/raw/data-tender-index_list-from-{initial_date}-to-{end_date}.csv"
    data_df.to_csv(data_df_csv_name, sep=',',
                   index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    today = datetime.today()
    yesterday = today - timedelta(days=22)
    initial_date = datetime.strftime(yesterday, '%d-%m-%Y')
    end_date = datetime.strftime(today, '%d-%m-%Y')
    extract(initial_date, end_date)

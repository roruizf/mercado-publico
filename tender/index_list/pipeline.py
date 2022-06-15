import argparse
from time import time
from extract import extract
from transform import transform
from load import load


def main(params):
    start_time = time()
    initial_date = params.initial_date
    end_date = params.end_date

    print('\nStarting extraction process...')
    extract(initial_date, end_date)
    print('\nExtraction done!')
    print('\nStarting transform process...')
    transform()
    print('\nTransform done!')
    print('\nStarting loading process...')
    load()
    print('\nLoading done!')

    end_time = time()

    print('Inserted file, took %.3f seconds' % (end_time - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Mercado libre tender list pipeline')

    parser.add_argument(
        'initial_date', help='Initial date to download data. Format %d-%m-%Y')
    parser.add_argument(
        'end_date', help='End date to download data. Format %d-%m-%Y')

    args = parser.parse_args()
    main(args)

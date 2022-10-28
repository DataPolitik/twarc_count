from statistics import mean, median, stdev
from typing import Generator, List, Dict

import ijson
import click
import pandas as pd
import numpy as np

COLUMNS_OPERATIONS: Dict[str, List[str]] = {
    'users': ['count'],
    'length': ['mean', 'min', 'max'],
    'languages': ['count']
}


def load_json_file(infile) -> Generator:
    json_file = ijson.items(infile, '', multiple_values=True)
    generator: Generator = (o for o in json_file)
    return generator


def sort_by_frequency(arr, is_asc_sort):
    unique_elements, frequency = np.unique(arr, return_counts=True)
    sorted_indexes = np.argsort(frequency)[::-1]
    sorted_elements = unique_elements[sorted_indexes]
    return sorted_elements[::-1] if is_asc_sort else sorted_elements


def output_value(output_string, file, is_verbose):
    click.echo(output_string, file=file)
    if is_verbose:
        click.echo(output_string)


@click.command()
@click.argument('infile', type=click.File('r'), default='-')
@click.argument('outfile', type=click.File('w'), default='-')
@click.option('-v', '--verbose', is_flag=True, flag_value=True, default=False,
              help='Output information in the terminal')
@click.option('-t', '--tweets', is_flag=True, flag_value=True, default=False, help='Count tweets')
@click.option('-u', '--users', is_flag=True, flag_value=True, default=False, help='Count users')
@click.option('-l', '--languages', is_flag=True, flag_value=True, default=False, help='Count languages')
@click.option('-e', '--length', is_flag=True, flag_value=True, default=False, help='Size of text')
@click.option('-p', '--pandas', type=click.Choice(['csv', 'json', 'pickle']),
              help='Exports the pandas dataframe in the specified format')
@click.option('-g', '--group', type=click.Choice(['users', 'languages', 'length']), multiple=True,
              help='Groups by one field. Shows mean, median and standard deviation per group.')
@click.option('-d', '--details', is_flag=True, flag_value=True, default=False, help='Get detailed information')
@click.option('-sa', '--sort-alphabetically', type=click.Choice(['asc', 'desc']),
              help='If -d option is enabled, sort information.')
@click.option('-sf', '--sort-frequency', type=click.Choice(['asc', 'desc']),
              help='If -d option is enabled, sort information by frequency.')
def count(infile,
          outfile,
          verbose,
          tweets,
          users,
          languages,
          length,
          pandas,
          group,
          details,
          sort_alphabetically,
          sort_frequency):

    if not tweets and not users and not languages and not length and not sort_frequency and not sort_alphabetically and not group:
        print("Nothing to count")
        return 0

    dataset_columns = {}
    dataset_columns_list = []
    if users:
        dataset_columns['users'] = COLUMNS_OPERATIONS['users']
        dataset_columns_list.append('users')
    if languages:
        dataset_columns['languages'] = COLUMNS_OPERATIONS['languages']
        dataset_columns_list.append('languages')
    if length:
        dataset_columns['length'] = COLUMNS_OPERATIONS['length']
        dataset_columns_list.append('length')

    list_of_dicts = []
    number_of_tweets = 0

    tweet_generator: Generator = load_json_file(infile)

    for tweet in tweet_generator:
        number_of_tweets = number_of_tweets + 1

        if len(dataset_columns) > 0:
            response_dictionary = {}
            if 'users' in dataset_columns:
                user_id = tweet['author']['id']
                response_dictionary['users'] = user_id
            if 'languages' in dataset_columns:
                tweet_language = tweet['lang']
                response_dictionary['languages'] = tweet_language
            if 'length' in dataset_columns:
                tweet_size = len(tweet['text'])
                response_dictionary['length'] = tweet_size

            list_of_dicts.append(response_dictionary)

    dataframe = pd.DataFrame(list_of_dicts)
    if sort_alphabetically:
        dataframe = dataframe.sort_values(by=dataset_columns_list, ascending=sort_alphabetically == 'asc')
    elif sort_frequency:
        sorted_list = ['freq'] + dataset_columns_list
        dataframe = dataframe\
            .assign(freq=dataframe.groupby(dataset_columns_list[0])[dataset_columns_list[0]].transform('count')) \
            .sort_values(by=sorted_list, ascending=sort_frequency == 'asc')
        dataframe = dataframe.drop(columns=['freq'])

    if tweets:
        output_string = "{}: {}".format("tweets", number_of_tweets)
        output_value(output_string, outfile, verbose)

    for column in dataset_columns:
        if column == 'length':
            output_string = '''average size: {}\nmedian size: {}\nstd size: {}\nmax size: {}\nmin size: {}'''.format(
                mean(dataframe[column]),
                median(dataframe[column]),
                stdev(dataframe[column]),
                max(dataframe[column]),
                min(dataframe[column])
            )
            output_value(output_string, outfile, verbose)
        else:
            unique_values = pd.unique(dataframe[column])
            output_string = "{}: {}".format(column, len(unique_values))
            output_value(output_string, outfile, verbose)
            if details:
                output_value(', '.join(unique_values), outfile, verbose)

    if group:
        if len(dataset_columns_list) == 1:
            dataframe = dataframe.groupby(dataset_columns_list[0]).size().reset_index()
        else:
            group_as_list = list(group)
            operations_dict = {}
            for key, operation in dataset_columns.items():
                operations_dict[key] = operation
            dataframe = dataframe.groupby(group_as_list).agg(operations_dict).reset_index()

        if pandas:
            if pandas == 'csv':
                dataframe.to_csv('dataframe.csv')
            elif pandas == 'json':
                dataframe.to_json('dataframe.json')
            elif pandas == 'pickle':
                dataframe.to_pickle('dataframe.pickle')

        output_string = "\n\nGroup by {}".format(group)
        output_value(output_string, outfile, verbose)
        output_string = dataframe.to_string(index=False)
        output_value(output_string, outfile, verbose)


if __name__ == '__main__':
    count()

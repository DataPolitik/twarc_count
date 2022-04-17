import json
import click
import pandas as pd

from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0


def output_value(output_string, file, is_verbose):
    click.echo(output_string, file=file)
    if is_verbose:
        click.echo(output_string)


@click.command()
@click.argument('infile', type=click.File('r'), default='-')
@click.argument('outfile', type=click.File('w'), default='-')
@click.option('-v', '--verbose', is_flag=True, flag_value=True, default=False, help='Output information in the terminal')
@click.option('-t', '--tweets', is_flag=True, flag_value=True, default=False, help='Count tweets')
@click.option('-u', '--users', is_flag=True, flag_value=True, default=False, help='Count users')
@click.option('-l', '--languages', is_flag=True, flag_value=True, default=False, help='Count languages')
@click.option('-d', '--details', is_flag=True, flag_value=True, default=False, help='Get detailed information')
@click.option('-sa', '--sort-alphabetically', type=click.Choice(['asc', 'desc']),
              help='If -d option is enabled, sort information.')
def count(infile, outfile, verbose, tweets, users, languages, details, sort_alphabetically):
    dataset_columns = []
    if users:
        dataset_columns.append('users')
    if languages:
        dataset_columns.append('languages')

    list_of_dicts = []
    number_of_tweets = 0

    for line in infile:
        tweet = json.loads(line)
        number_of_tweets = number_of_tweets + 1

        if len(dataset_columns) > 0:
            response_dictionary = {}
            if 'users' in dataset_columns:
                user_id = tweet['author']['id']
                response_dictionary['users'] = user_id
            if 'languages' in dataset_columns:
                tweet_language = detect(tweet['text'])
                response_dictionary['languages'] = tweet_language

            list_of_dicts.append(response_dictionary)

    dataframe = pd.DataFrame(list_of_dicts)

    if tweets:
        output_string = "{}: {}".format("tweets", number_of_tweets)
        output_value(output_string, outfile, verbose)

    for column in dataset_columns:
        unique_values = pd.unique(dataframe[column])
        output_string = "{}: {}".format(column, len(unique_values))
        output_value(output_string, outfile, verbose)
        if details:
            if sort_alphabetically:
                unique_values.sort() if sort_alphabetically == 'asc' else unique_values[::-1].sort()
            output_value(', '.join(unique_values), outfile, verbose)


if __name__ == '__main__':
    count()

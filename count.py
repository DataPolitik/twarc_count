import json
import click
import pandas as pd

from textblob import TextBlob


def output_value(output_string, file):
    click.echo(output_string, file=file)
    click.echo(output_string)


@click.command()
@click.argument('infile', type=click.File('r'), default='-')
@click.argument('outfile', type=click.File('w'), default='-')
@click.option('-t', '--tweets', is_flag=True, flag_value=True, default=False, help='Count tweets')
@click.option('-u', '--users', is_flag=True, flag_value=True, default=False, help='Count users')
@click.option('-l', '--language', is_flag=True, flag_value=True, default=False, help='Count languages')
def count(infile, outfile, tweets, users, languages):
    dataset_columns = []
    if users:
        dataset_columns.append('users')
    if languages:
        dataset_columns.append('languages')

    dataframe = pd.DataFrame(columns=dataset_columns)

    for line in infile:
        tweet = json.loads(line)
        response_dictionary = {}

        if 'users' in dataset_columns:
            user_id = tweet['user_id']
            response_dictionary['user'] = user_id
        if 'languages' in dataset_columns:
            tweet_language = TextBlob(tweet['text'])
            response_dictionary['language'] = tweet_language

        dataframe.append(response_dictionary)

    for column in dataset_columns:
        unique_values = pd.unique(dataframe[column])
        output_string = "{}: {}\n".format(column, len(unique_values))
        output_value(output_string, outfile)

    if tweets:
        output_string = "{}: {}\n".format("Tweets", len(dataframe.index))
        output_value(output_string, outfile)


if __name__ == '__main__':
    count()
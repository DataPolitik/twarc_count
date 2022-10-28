# Twarc-count
## A counter plugin for twarc

This script reads a json file generated by Twarc and count specific information defined by the user.

## Requirements

Twarc-count requires Python 3.7 or greater and pip.

## Installation

You need to clone this repository.

`git clone https://github.com/DataPolitik/twarc_count.git`

And then, move to the folder **twarc_count**. Then, install all modules required by the script:

`pip install -r requirements.txt`

## Usage

`count.py  <INFILE> -o <OUTFILE> [-f [FIELDS] ] [ -e [EXTENSION]]`

* **-v** | **- --verbose**: Output information in the terminal.
* **-t** | **- --tweets**: Count tweets.
* **-u* | **- -users**: Size of the text.
* **-l** | **- -languages**: Count languages.
* **-e** | **- -length**: Size of the text.
* **-p** | **- -pandas**: Exports the pandas dataframe in the specified format.
* **-g** | **- -group**: Groups by one field. Shows mean, median and standard deviation per group.
* **-d** | **- -details**: Get detailed information. If available.
* **-sa** | **- -sort-alphabetically**: If -d option is enabled, sort information. You must indicate 'asc' or 'desc' after the parameter.
* **-sf** | **- -sort-frequency**: If -d option is enabled, sort information. You must indicate 'asc' or 'desc' after the parameter.
 
## Fields

You can check the Twitter API documentation (https://developer.twitter.com/en/docs/twitter-api/fields) for
more information about fields names and expansions.

## Examples

### Count number of tweets in the file

`count.py  examples/results.json -v -t`

### Count languages of tweets in the file

`count.py  examples/results.json -v -l`

If you want to obtain a detailed list of languages:

`count.py  examples/results.json -v -ld`

### Group by users

`count.py  examples/results.json -v -g users`

### Exports a pandas dataframe

`count.py  examples/results.json -v -g users -p csv`



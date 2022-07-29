# facebook_scrapper
scrapes a post and commentaries from a given facebook page

specify the page/group you wish to scrape and where you want CSV files to be stored through command-line arguments.
It also separates your App ID and App secret from the code; now, you have to store these credentials in a separate file.

## Usage

To scrape posts from a page:

`python3 run.py --page <page name> --cred <path to credential file> --posts-output <filepath>`

To scrape both posts and comments:

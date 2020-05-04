import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time

file_dir = 'C:/Users/aline/Projects/UC_Berkeley/Module_8/'

#Open files:
with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
    
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
    
ratings = pd.read_csv(f'{file_dir}ratings.csv')

def auto_ETL(wiki_movies_raw, kaggle_metadata, ratings):
    
    # -------------------------------------- CLEAN WIKIPEDIA DATA --------------------------------------
    # Filter wiki_movies_raw for only movies with a director, an IMDb link and not a TV show
    wiki_movies = [movie for movie in wiki_movies_raw 
               if ('Director' in movie or 'Directed by' in movie) 
               and 'imdb_link' in movie
               and 'No. of episodes' not in movie]
    
    # Create function to clean alternative titles from the movie data.
    alt_titles_list = ['Also known as','Arabic','Cantonese','Chinese','French', 
                   'Hangul','Hebrew','Hepburn','Japanese','Literally','Mandarin',
                   'McCune–Reischauer','Original title','Polish','Revised Romanization',
                   'Romanized','Russian','Simplified','Traditional','Yiddish']
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles_dict = {}
        for key in alt_titles_list:
            if key in movie:
                alt_titles_dict[key] = movie[key]
                movie.pop(key)
        if len(alt_titles_dict) > 0:
            movie['alt_titles'] = alt_titles_dict
        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
    
        return movie
    
    wiki_movies_df = pd.DataFrame([clean_movie(movie) for movie in wiki_movies])
    
    # Extract imdb id from url. Create another column 'imdb_id'
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

    # Remove duplicates (rows with the same IMDB ID):
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # Keep only columns that have less than 90% null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < 0.9*len(wiki_movies_df)]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    # Parse Box Office column -----------------------------------------------------------------------
    # Make a variable that holds the non-null values of Box Office in the DataFrame, converting lists to strings
    box_office = wiki_movies_df['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (for box office given in ranges)
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Create function to turn the extracted values into a numeric value
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    # Apply parse_dollars to the first column in the DataFrame returned by str.extract
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # Drop 'Box office' column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # Parse Budget column -------------------------------------------------------------------------------
    # Make a variable that holds the non-null values of Budget in the DataFrame, converting lists to strings
    budget = wiki_movies_df['Budget'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Remove the citation references
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    
    # Parse the budget values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Drop the original Budget column  column from the dataset
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # Parse Release Date column -------------------------------------------------------------------------
    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # (i.e., January 1, 2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    # (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    # (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    # Four-digit year
    date_form_four = r'\d{4}'
    
    # Parse the release dates using the built-in to_datetime() method
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    # Parse Running Time column -------------------------------------------------------------------------
    # Make a variable that holds the non-null values of Running time in the DataFrame, converting lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # We only want to extract digits, and we want to allow for both possible patterns
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
    # Coercing the errors will turn the empty strings into Not a Number (NaN), then fillna() changes the NaNs to zeros.
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    # Convert the hour and minute capture groups to minutes if the pure minutes capture group is zero
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # Drop original Running Time column from the dataset
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    

    # -------------------------------------- CLEAN KAGGLE METADATA ------------------------------------------
    
    # keep rows where the adult column is False, and then drop the adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    
    # Convert Video column to boolean
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    # Convert Budget, ID and Popularity columns to numeric
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    
    # Convert release_date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    

    
    # -------------------------------------- CLEAN RATINGS DATA ---------------------------------------------
    
    # Specify in to_datetime() that the origin is 'unix' and the time unit is seconds
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        

    # ------------------------------ MERGE WIKIPEDIA AND KAGGLE DATASETS ------------------------------------
    
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # Drop redundant columns and fill missing values with zeros
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # Reorder the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]
    
    # Rename the columns to be consistent
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    
    
    # -------------------------------------- MERGE WITH RATINGS DATASET ----------------------------------------
    
    # Group ratings
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    
    # Prepend rating_ to each column
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    # Left merge Wiki+Kaggle data with ratings
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # Fill missing values in with zeros
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    
     # -------------------------------------------- LOAD TO SQL ---------------------------------------------- 
    
    # Connection string
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    
    # Create the database engine
    engine = create_engine(db_string)
    
    # Load Wikipedia + Kaggle data
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    # Import the Ratings Data divided into “chunks”
#     rows_imported = 0
#     # get the start_time from time.time()
#     start_time = time.time()
#     for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
#         print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
#         data.to_sql(name='ratings', con=engine, if_exists='append')
#         rows_imported += len(data)

#         # add elapsed time to final print out
#         print(f'Done. {time.time() - start_time} total seconds elapsed')


auto_ETL(wiki_movies_raw, kaggle_metadata, ratings)


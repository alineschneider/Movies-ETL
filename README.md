# Movies-ETL

### Wikipedia

* Maintained only movies that had a Director (column 'Director' or 'Directed by') and an IMDB link;

* Deleted rows that contained a value in the 'No. of episodes' column, to remove TV shows from the movie data.

* Kept columns that have less than 90% null values;

* Parsed and adjusted box office and budget columns to fit forms one ($123.4 million) or two ($123,456,789); removed budgets given in ranges; dropped 30 movie budgets that did not fit the defined forms.

### Kaggle Metadata

* Removed rows containing adult movies;

### Merged Datasets
Competing data:
Wiki | Movielens | Resolution
------ | ------ | ------
title_wiki | title_kaggle | Drop Wikipedia.
running_time | runtime | Keep Kaggle; fill in zeros with Wikipedia data.
budget_wiki | budget_kaggle | Keep Kaggle; fill in zeros with Wikipedia data.
box_office | revenue | Keep Kaggle; fill in zeros with Wikipedia data.
release_date_wiki | release_date_kaggle | Drop Wikipedia.
Language | original_language | Drop Wikipedia.
Production company(s) | production_companies | Drop Wikipedia. 
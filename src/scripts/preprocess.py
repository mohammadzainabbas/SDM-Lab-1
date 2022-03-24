#!/usr/bin/env python

import pandas as pd
from os import getcwd
from os.path import join, abspath, pardir
import re

## Helper methods

def cast_columns(df, cols, __type):
    df[cols] = df[cols].astype(__type)
    return df
def flatten(t):
    return [item for sublist in t for item in sublist]

def split_str_and_concat(df, col_name, sep="; "):
    return pd.DataFrame(df[col_name].str.split(sep).tolist(), index=keywords_df.index).stack()

def filter_countries(df):
    """
    Not perfect but filter out almost 99.9% countries for our dataset
    """
    # Find "(" or ")" or any digit
    regex = re.compile(r"(\(|\))|(\@)|(\d+)+", re.S)
    def _filter(regex, x):
        return None if regex.search(x) else x
    return pd.Series([_filter(regex, str(x)) for x in df['country']])

def filter_department(df):
    """
    Not perfect solution but good for more than 85%-90% cases. 
    """
    def _filter(a):
        aa = [x for x in a.split(",") if str(x).find("Dep") != -1 or str(x).find("School") != -1]
        return aa[0] if len(aa) else None
    return pd.Series([_filter(str(x)) for x in df['name']])

def main():
    """
    main function for preprocessing the dataset
    """
    ### Setup Path(s)
    parent_dir = abspath(join(join(getcwd(), pardir), pardir))
    data_dir = join(parent_dir, "data")
    scripts_dir = join(parent_dir, "src", "scripts")
    data_file = join(data_dir, "publications.csv")

    ### Load data
    df = pd.read_csv(data_file)

    ## Define all the columns needed

    journals_cols = ['Year', 'Source title', 'Volume']
    keyword_cols = ['Author Keywords', 'Index Keywords']
    affiliation_cols = ['Affiliations']
    authors_cols = ['Authors', 'Author(s) ID', 'Affiliations', 'Authors with affiliations', 'Author Keywords']
    document_cols = ['Author(s) ID', 'Title', 'Source title', 'Art. No.', 'Cited by', 'DOI', 'Abstract', 'Author Keywords', 'Index Keywords', 'Document Type']

    # `Journals`

    journals_df = df[journals_cols]

    journals_df = cast_columns(journals_df, ['Source title', 'Volume'], pd.StringDtype())
    journals_df.rename(columns={'Year': 'year', 'Source title': 'name', 'Volume': 'volume'}, inplace=True)

    journals_df.drop_duplicates(inplace=True)
    journals_df.to_csv(join(data_dir, "journals.csv"), index=False)


    # `Keywords`
    keywords_df = df[keyword_cols]
    keywords_df = cast_columns(keywords_df, keyword_cols, pd.StringDtype())
    keywords_df.dropna(inplace=True)
    author_keywords = split_str_and_concat(keywords_df, 'Author Keywords')
    index_keywords = split_str_and_concat(keywords_df, 'Index Keywords')
    all_keywords = author_keywords.append(index_keywords)
    all_keywords.drop_duplicates(keep='first', inplace=True)

    keywords_df = pd.DataFrame({ 'name': all_keywords })
    keywords_df.to_csv(join(data_dir, "keywords.csv"), index=False)

    # `Affiliations`
    affiliations_df = df[affiliation_cols]
    affiliations_df = cast_columns(affiliations_df, affiliation_cols, pd.StringDtype())
    
    all_affiliations = pd.DataFrame(affiliations_df['Affiliations'].str.split("; ").tolist()).stack()
    all_affiliations.drop_duplicates(keep='first', inplace=True)
    affiliations_df = all_affiliations.to_frame(name='name').reset_index(drop=True)
    affiliations_df.to_csv(join(data_dir, "affiliations.csv"), index=False)

    # `Documents`
    document_df = df[document_cols]
    document_df.rename(columns=
        {'DOI':'doi', 
        'Author(s) ID': 'author_ids', 
        'Art. No.':'article_no', 
        'Title':'title',
        'Abstract':'abstract',
        'Author Keywords':'author_keywords',
        'Index Keywords':'index_keywords',
        'Document Type':'document_type',
        'Cited by':'cited_count',
        'Source title':'source_title'}, inplace=True)

    document_df.drop(columns=['author_ids', 'author_keywords', 'index_keywords', 'source_title'], inplace=True)
    document_df['cited_count'].fillna(0, inplace=True)

    # Since, we have NaN values for `doi`, so we are using index as an identifier
    document_df.reset_index(inplace=True)
    document_df.rename(columns={'index':'document_id'}, inplace=True)
    document_df.to_csv(join(data_dir, "documents.csv"), index=False)
    
    # `Authors`
    authors_df = df[authors_cols]
    authors_df.rename(columns=
        {'Authors':'name', 
        'Author(s) ID': 'author_ids',
        'Affiliations':'affiliation',
        'Author Keywords':'author_keywords'}, inplace=True)
    authors = list()
    def filter_authors(x):
        def filter_affiliations(y):
            try:            
                aff = y.split("., ")
                return "".join(aff[1:])
            except ValueError as e:
                print(y)
                raise e
        names = x['name'].split(",") if x['name'] else None
        author_ids = x['author_ids'].split(";") if x['author_ids'] else None
        author_keywords = x['author_keywords'].split(";") if x['author_keywords'] and isinstance(x['author_keywords'], str) else None
        auth_affiliations = x['Authors with affiliations'].split(";") if x['Authors with affiliations'] else None
        if not len(author_ids[len(author_ids) - 1]): del author_ids[len(author_ids) - 1]
        
        if len(names) == len(author_ids) == len(auth_affiliations):
            for index, name in enumerate(names):
                author = dict() 
                author['author_id'] = author_ids[index]
                author['name'] = name
                author['affiliations'] = filter_affiliations(auth_affiliations[index])
                author['keywords'] = author_keywords
                authors.append(author)
    _ = authors_df.apply(lambda x: filter_authors(x), axis=1)

    authors_df = pd.DataFrame(authors)
    
    _authors = dict()
    def filter_authors_affiliation(x):
        if x['author_id'] not in _authors.keys(): _authors[x['author_id']] = dict()

        _authors[x['author_id']]['name'] = x['name']
        
        if x['affiliations']:        
            if 'affiliations' not in _authors[x['author_id']].keys():
                _authors[x['author_id']]['affiliations'] = list()
            _authors[x['author_id']]['affiliations'].append(x['affiliations'])
        
        if x['keywords']:        
            if 'keywords' not in _authors[x['author_id']].keys():
                _authors[x['author_id']]['keywords'] = list()
            _authors[x['author_id']]['keywords'].extend(x['keywords'])

    _ = authors_df.apply(lambda x: filter_authors_affiliation(x), axis=1)
    auth = list()
    for key, value in _authors.items():
        a = dict()
        a['author_id'] = key
        a.update(value)
        auth.append(a)
    authors_df = pd.DataFrame(auth)
    authors_df.drop(columns=['affiliations', 'keywords'], inplace=True)
    authors_df.drop_duplicates(inplace=True)
    authors_df.to_csv(join(data_dir, "authors.csv"), index=False)

    ## For relations (bridge tables)

    # `Document && Author`
    cols = ['author_ids', 'document_id']
    doc_auth_df = document_df[cols]
    doc_authors = list()
    def filter_doc_authors(x):
        author_ids = x['author_ids'] if x['author_ids'] and isinstance(x['author_ids'], list) else None
        document_id = x['document_id']
        
        for index, author_id in enumerate(author_ids):
            doc_auth = dict()
            doc_auth['author_id'] = author_id
            doc_auth['document_id'] = document_id
            doc_authors.append(doc_auth)

    _ = doc_auth_df.apply(lambda x: filter_doc_authors(x), axis=1)
    doc_auth_df = pd.DataFrame(doc_authors)
    doc_auth_df.to_csv(join(data_dir, "document_author.csv"), index=False)


    # `Document && Keywords`
    cols = ['keywords', 'document_id']
    doc_keywords_df = document_df[cols]
    doc_keywords_df.drop(doc_keywords_df[doc_keywords_df['keywords'].isnull()].index, inplace=True)
    doc_keywords = list()
    def filter_doc_keywords(x):
        keywords = x['keywords'] if x['keywords'] and isinstance(x['keywords'], list) else None
        document_id = x['document_id']
        
        for index, keyword in enumerate(keywords):
            doc_keyword = dict()
            doc_keyword['keyword'] = keyword
            doc_keyword['document_id'] = document_id
            doc_keywords.append(doc_keyword)
    _ = doc_keywords_df.apply(lambda x: filter_doc_keywords(x), axis=1)
    doc_keywords_df = pd.DataFrame(doc_keywords)
    doc_keywords_df.to_csv(join(data_dir, "document_keyword.csv"), index=False)

    # `Author && Keywords`

    cols = ['author_id', 'keywords']
    auth_keywords_df = authors_df[cols]

    auth_keywords_df.drop(auth_keywords_df[auth_keywords_df['keywords'].isnull()].index, inplace=True)
    auth_keywords = list()
    def filter_auth_keywords(x):
        keywords = x['keywords'] if x['keywords'] and isinstance(x['keywords'], list) else None
        author_id = x['author_id']
        
        for index, keyword in enumerate(keywords):
            auth_keyword = dict()
            auth_keyword['keyword'] = keyword
            auth_keyword['author_id'] = author_id
            auth_keywords.append(auth_keyword)
    _ = auth_keywords_df.apply(lambda x: filter_auth_keywords(x), axis=1)
    auth_keywords_df = pd.DataFrame(auth_keywords)
    auth_keywords_df.to_csv(join(data_dir, "author_keyword.csv"), index=False)

    # `Author && Affiliation`

    cols = ['author_id', 'affiliations']
    auth_affiliations_df = authors_df[cols]
    auth_affiliations_df.drop(auth_affiliations_df[auth_affiliations_df['affiliations'].isnull()].index, inplace=True)
    auth_affiliations = list()
    def filter_auth_affiliations(x):
        affiliations = x['affiliations'] if x['affiliations'] and isinstance(x['affiliations'], list) else None
        author_id = x['author_id']
        
        for index, affiliation in enumerate(affiliations):
            auth_affiliation = dict()
            auth_affiliation['affiliation'] = affiliation
            auth_affiliation['author_id'] = author_id
            auth_affiliations.append(auth_affiliation)
    _ = auth_affiliations_df.apply(lambda x: filter_auth_affiliations(x), axis=1)

    auth_affiliations_df.to_csv(join(data_dir, "author_affiliation.csv"), index=False)

    # `Document && Journal`
    _df = df.copy()
    _df.rename(columns={"Title": "title", "Source title": "source_title"}, inplace=True)
    c = ['title', 'source_title']
    _df = _df[c]
    doc_journals_df = pd.merge(_df, document_df, on='title')
    doc_journals_df.drop(columns=['title', 'article_no', 'cited_count', 'doi', 'abstract', 'document_type'], inplace=True)
    doc_journals_df.to_csv(join(data_dir, "document_journal.csv"), index=False)

if __name__ == "__main__":
    main()
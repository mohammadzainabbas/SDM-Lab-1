#!/usr/bin/env python

import pandas as pd
from os import getcwd
from os.path import join, abspath, pardir
import re

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
df.head(3)

## Define all the columns needed

journals_cols = ['Year', 'Source title', 'Volume']
keyword_cols = ['Author Keywords', 'Index Keywords']
affiliation_cols = ['Affiliations']
authors_cols = ['Authors', 'Author(s) ID', 'Affiliations', 'Authors with affiliations', 'Author Keywords']
document_cols = ['Author(s) ID', 'Title', 'Source title', 'Art. No.', 'Cited by', 'DOI', 'Abstract', 'Author Keywords', 'Index Keywords', 'Document Type']


# ##### Helper methods

# In[5]:


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


# `Journals`

# In[6]:


journals_df = df[journals_cols]
journals_df.dtypes


# In[7]:


journals_df.head()


# In[8]:


journals_df = cast_columns(journals_df, ['Source title', 'Volume'], pd.StringDtype())
journals_df.rename(columns={'Year': 'year', 'Source title': 'name', 'Volume': 'volume'}, inplace=True)
journals_df.dtypes


# In[9]:


journals_df.head()


# Which column has null values ??

# In[10]:


journals_df.isnull().any()


# See some samples of `volume` being null

# In[11]:


journals_df[journals_df['volume'].isnull()].head()


# In[12]:


journals_df.drop_duplicates(inplace=True)


# Save as `journals.csv` file

# In[13]:


journals_df.to_csv(join(data_dir, "journals.csv"), index=False)


# `Keywords`

# In[14]:


keywords_df = df[keyword_cols]
keywords_df.dtypes


# In[15]:


keywords_df = cast_columns(keywords_df, keyword_cols, pd.StringDtype())
keywords_df.dtypes


# In[16]:


keywords_df.head()


# In[17]:


keywords_df.dropna(inplace=True)
author_keywords = split_str_and_concat(keywords_df, 'Author Keywords')
index_keywords = split_str_and_concat(keywords_df, 'Index Keywords')


# In[18]:


all_keywords = author_keywords.append(index_keywords)
all_keywords.drop_duplicates(keep='first', inplace=True)


# In[19]:


keywords_df = pd.DataFrame({ 'name': all_keywords })
keywords_df.head()


# Save as `keywords.csv` file

# In[22]:


keywords_df.to_csv(join(data_dir, "keywords.csv"), index=False)


# `Affiliations`

# In[35]:


affiliations_df = df[affiliation_cols]
affiliations_df.dtypes


# In[36]:


affiliations_df = cast_columns(affiliations_df, affiliation_cols, pd.StringDtype())
affiliations_df.dtypes


# In[37]:


all_affiliations = pd.DataFrame(affiliations_df['Affiliations'].str.split("; ").tolist()).stack()
all_affiliations.drop_duplicates(keep='first', inplace=True)
affiliations_df = all_affiliations.to_frame(name='name').reset_index(drop=True)


# In[38]:


# affiliations_df['country'] = affiliations_df['name'].str.rsplit(',', n=2, expand=True)[2]
# affiliations_df['country'] = filter_countries(affiliations_df)

# affiliations_df['dept_name'] = filter_department(affiliations_df)


# In[39]:


affiliations_df.head()


# Save as `affiliations.csv` file

# In[40]:


affiliations_df.to_csv(join(data_dir, "affiliations.csv"), index=False)


# `Documents`

# In[41]:


document_df = df[document_cols]


# In[42]:


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


# In[43]:


document_df.head(2)


# In[44]:


document_df.drop(columns=['author_ids', 'author_keywords', 'index_keywords', 'source_title'], inplace=True)


# In[46]:


document_df['cited_count'].fillna(0, inplace=True)


# In[47]:


document_df.head(2)


# Since, we have NaN values for `doi`, so we are using index as an identifier

# In[48]:


document_df.reset_index(inplace=True)
document_df.rename(columns={'index':'document_id'}, inplace=True)
document_df.head(2)


# Save as `documents.csv` file

# In[49]:


document_df.to_csv(join(data_dir, "documents.csv"), index=False)


# `Authors`

# In[50]:


authors_df = df[authors_cols]
authors_df.head()


# In[51]:


authors_df.rename(columns=
              {'Authors':'name', 
               'Author(s) ID': 'author_ids',
               'Affiliations':'affiliation',
               'Author Keywords':'author_keywords'}, inplace=True)


# In[52]:


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


# In[53]:


_ = authors_df.apply(lambda x: filter_authors(x), axis=1)


# In[54]:


len(authors)


# In[55]:


authors_df = pd.DataFrame(authors)


# In[56]:


authors_df.head(2)


# In[57]:


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


# In[58]:


_ = authors_df.apply(lambda x: filter_authors_affiliation(x), axis=1)


# In[59]:


# authors_df.groupby('author_id').apply(lambda x: filter_authors_affiliation(x))
auth = list()
for key, value in _authors.items():
    a = dict()
    a['author_id'] = key
    a.update(value)
    auth.append(a)


# In[60]:


authors_df = pd.DataFrame(auth)


# In[64]:


authors_df.drop(columns=['affiliations', 'keywords'], inplace=True)


# In[66]:


authors_df.drop_duplicates(inplace=True)


# Save as `authors.csv` file

# In[67]:


authors_df.to_csv(join(data_dir, "authors.csv"), index=False)


# ##### For relations (bridge tables)

# `Document && Author`

# In[ ]:


cols = ['author_ids', 'document_id']
doc_auth_df = document_df[cols]
doc_auth_df.head(2)


# In[94]:


doc_authors = list()
def filter_doc_authors(x):
    author_ids = x['author_ids'] if x['author_ids'] and isinstance(x['author_ids'], list) else None
    document_id = x['document_id']
    
    for index, author_id in enumerate(author_ids):
        doc_auth = dict()
        doc_auth['author_id'] = author_id
        doc_auth['document_id'] = document_id
        doc_authors.append(doc_auth)


# In[95]:


_ = doc_auth_df.apply(lambda x: filter_doc_authors(x), axis=1)


# Save as `document_author.csv` file

# In[118]:


doc_auth_df = pd.DataFrame(doc_authors)
doc_auth_df.to_csv(join(data_dir, "document_author.csv"), index=False)


# `Document && Keywords`

# In[107]:


cols = ['keywords', 'document_id']
doc_keywords_df = document_df[cols]
doc_keywords_df.head(2)


# Remove rows where there are no keywords

# In[108]:


doc_keywords_df.drop(doc_keywords_df[doc_keywords_df['keywords'].isnull()].index, inplace=True)


# In[111]:


doc_keywords_df.head(2)


# In[112]:


doc_keywords = list()
def filter_doc_keywords(x):
    keywords = x['keywords'] if x['keywords'] and isinstance(x['keywords'], list) else None
    document_id = x['document_id']
    
    for index, keyword in enumerate(keywords):
        doc_keyword = dict()
        doc_keyword['keyword'] = keyword
        doc_keyword['document_id'] = document_id
        doc_keywords.append(doc_keyword)


# In[113]:


_ = doc_keywords_df.apply(lambda x: filter_doc_keywords(x), axis=1)


# Save as `document_keyword.csv` file

# In[122]:


doc_keywords_df = pd.DataFrame(doc_keywords)
doc_keywords_df.to_csv(join(data_dir, "document_keyword.csv"), index=False)


# `Author && Keywords`

# In[123]:


cols = ['author_id', 'keywords']
auth_keywords_df = authors_df[cols]
auth_keywords_df.head(2)


# In[125]:


auth_keywords_df.drop(auth_keywords_df[auth_keywords_df['keywords'].isnull()].index, inplace=True)


# In[126]:


auth_keywords_df.head()


# In[127]:


auth_keywords = list()
def filter_auth_keywords(x):
    keywords = x['keywords'] if x['keywords'] and isinstance(x['keywords'], list) else None
    author_id = x['author_id']
    
    for index, keyword in enumerate(keywords):
        auth_keyword = dict()
        auth_keyword['keyword'] = keyword
        auth_keyword['author_id'] = author_id
        auth_keywords.append(auth_keyword)


# In[128]:


_ = auth_keywords_df.apply(lambda x: filter_auth_keywords(x), axis=1)


# Save as `author_keyword.csv` file

# In[130]:


auth_keywords_df = pd.DataFrame(auth_keywords)
auth_keywords_df.to_csv(join(data_dir, "author_keyword.csv"), index=False)


# `Author && Affiliation`

# In[221]:


cols = ['author_id', 'affiliations']
auth_affiliations_df = authors_df[cols]


# In[224]:


auth_affiliations_df.head(2)


# In[223]:


auth_affiliations_df.drop(auth_affiliations_df[auth_affiliations_df['affiliations'].isnull()].index, inplace=True)


# In[228]:


auth_affiliations = list()
def filter_auth_affiliations(x):
    affiliations = x['affiliations'] if x['affiliations'] and isinstance(x['affiliations'], list) else None
    author_id = x['author_id']
    
    for index, affiliation in enumerate(affiliations):
        auth_affiliation = dict()
        auth_affiliation['affiliation'] = affiliation
        auth_affiliation['author_id'] = author_id
        auth_affiliations.append(auth_affiliation)


# In[229]:


_ = auth_affiliations_df.apply(lambda x: filter_auth_affiliations(x), axis=1)


# In[114]:


auth_affiliations_df = pd.read_csv(join(data_dir, "authors.csv"))
auth_affiliations_df.shape


# In[115]:


auth_affiliations_df.drop_duplicates().shape


# In[108]:


auth_affiliations_df.drop_duplicates(inplace=True)


# In[109]:


# auth_affiliations_df = pd.DataFrame(auth_affiliations)
auth_affiliations_df.to_csv(join(data_dir, "document_keyword.csv"), index=False)


# Save as `author_keyword.csv` file

# In[94]:


# auth_affiliations_df = pd.DataFrame(auth_affiliations)
auth_affiliations_df.to_csv(join(data_dir, "author_affiliation.csv"), index=False)


# `Document && Journal`

# In[79]:


_df = df.copy()


# In[80]:


_df.rename(columns={"Title": "title", "Source title": "source_title"}, inplace=True)


# In[81]:


c = ['title', 'source_title']


# In[82]:


_df = _df[c]


# In[85]:


doc_journals_df = pd.merge(_df, document_df, on='title')


# In[86]:


doc_journals_df.head(2)


# In[87]:


doc_journals_df.drop(columns=['title', 'article_no', 'cited_count', 'doi', 'abstract', 'document_type'], inplace=True)


# In[88]:


doc_journals_df.head(2)


# Save as `document_journal.csv` file

# In[91]:


doc_journals_df.to_csv(join(data_dir, "document_journal.csv"), index=False)


# In[ ]:





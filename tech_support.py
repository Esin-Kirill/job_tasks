from configs import *
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics import pairwise_distances
from pymorphy2 import MorphAnalyzer
from datetime import datetime
from bs4 import BeautifulSoup
from jira import JIRA
import requests
import html2text
import itertools
import pandas as pd
import warnings
import time
import re
import csv
import os

#IGNORE WARNINGS
warnings.filterwarnings('ignore')

#МЕНЯЕМ РАБОЧУЮ ДИРЕКТОРИЮ
path = r'<replaced>'
os.chdir(path)

# CONFLUENCE CONNECTION

# JIRA CONNECTION
jira_options = {'server': '<replaced>'}
JIRA = JIRA(options=jira_options, basic_auth=(LOGIN, PASSWORD))

# FUNCTIONS
def log_time():
    return time.strftime('%H:%M')


def process_text(text):
    text = text.replace('<', '').replace('>', '')
    text = text.replace('&gt;', '').replace('\.', '')
    return text


def process_df_html(df):
    # PROCESS HEADERS
    df.loc[0, :] = [x.text for x in df.loc[0, :]]
    df.columns = df.loc[0]
    df = df[1:]
    
    # PROCESS BODY
    answer = '<replaced>'
    df[answer] = df[answer].apply(lambda x: html2text.html2text(str(x)))
    df[answer] = df[answer].apply(process_text)

    cols = df.columns.to_list()
    cols.remove(answer)
    for col in cols:
        df[col] = df[col].apply(lambda x: x.text)
        
    return df


# FIND PAGE CHILDREN
def find_page_children(page_id, username, password):
    URL = f'<replaced>/rest/api/content/search?cql=parent={page_id}'
    result = requests.get(URL, auth=(username, password))
    list_pages = [page['id'] for page in result.json()['results']]
    return list_pages


# GET TABLE FROM CONFLUENCE
def get_page_table(page_id, username, password):
    # MAKE REQUEST
    URL = f'<replaced>/rest/api/content/{page_id}?expand=body.storage'
    response = requests.get(URL, auth=(username, password))

    # GET TABLES
    body = response.json()["body"]["storage"]["value"]
    list_dfs = [[[cell for cell in row("th") + row("td")] for row in table("tr")]
                for table in BeautifulSoup(body, features='html.parser')("table")]
    
    # PROCESS
    df = pd.DataFrame(list_dfs[0])
    df = process_df_html(df)
    
    return df


def get_project_markers(project, username, password):
    # PROJECT
    dict_pages = {'<replaced>':'<replaced>', '<replaced>':'<replaced>'}
    parent_pages = dict_pages[project]
    
    # FIND ALL CHILDREN
    list_pages = []
    for page_id in parent_pages:
        list_pages += find_page_children(page_id, username, password)
       
    # UNION TABLES
    df_all = pd.DataFrame()
    for page_id in list_pages:
        try:
            df_tmp = get_page_table(page_id, username, password)
            df_all = pd.concat([df_tmp, df_all], axis=0)
        except:
            continue
      
    # PROCESS
    dict_rename = {'<replaced>':'<replaced>',
                   '<replaced>':'marker',
                   '<replaced>':'key_words'}
    df_all = df_all.rename(columns=dict_rename)
    df_all = df_all[df_all['<replaced>']=='<replaced>']

    return df_all

# LOAD ISSUES
def load_issues(jira, project, nrows, file_to_write):
    # QUERY PARAMS
    issue_fields = 'created,description,labels,project,status'
    jql_search = f"project = {project} and status in ('Открыт') and labels != Прасковья"
    issues = jira.search_issues(jql_search, fields=issue_fields, maxResults=10)
    
    cols = ['id', 'key', 'created', 'description', 'labels', 'project', 'status']
    
    # WRITE TO FILE
    with open(file_to_write, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=cols, delimiter=';')
        writer.writeheader()
        startAt = 0
        
        # QUERY
        while len(issues) != 0:
            issues = jira.search_issues(jql_search, fields=issue_fields, maxResults=nrows, startAt=startAt)
            startAt += len(issues)
            
            for issue in issues:
                p0 = issue.raw['id']
                p1 = issue.raw['key']
                p2 = datetime.strptime(issue.raw['fields']['created'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
                p3 = issue.raw['fields']['description']
                p4 = issue.raw['fields']['labels']
                p5 = issue.raw['fields']['project']['key']
                p6 = issue.raw['fields']['status']['name']

                # WRITE TO FILE
                row = [p0, p1, p2, p3, p4, p5, p6]
                writer.writerow(dict(zip(cols, row)))
            
    print(log_time(), f'<replaced> {project}: {startAt}')
    return startAt


def clear_text(text):
    signs = ["<replaced>']
    pattern = r"[^а-яёa-z]"
    
    if text and type(text)==str:
        # DELETE DEFAULT CONSTRUCTIONS
        for sign in signs:
            search = re.search(sign, text,  flags=re.IGNORECASE)
            if search:
                text = text.replace(search[0], '')

        # LEAVE ONLY WORDS
        text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
        return text
    else:
        return text


def ecp_esmv(tokens, text):
    try:
        words = ['<replaced>']
        for word in words:
            if word in text and word not in tokens:
                tokens += [word]
        return tokens
    except:
        tokens


def lemmatize(text, morph):
    try:
        tokens = []
        
        for token in text.split():
            if token:
                token = token.strip()
                token = morph.normal_forms(token)[0]
                if len(token) >= 3:
                    tokens.append(token)
                    
        tokens = ecp_esmv(tokens, text)
        return tokens
    
    except:
        return []


def collect_marker_words(value):
    marker_words = [word.strip().lower() for word in value.split(';')]
    for word in marker_words:
        if ' ' in word or '_' in word:
            marker_words.remove(word)

    return marker_words


def collect_marker_words_two(value):
    words_two = []
    marker_words = [word.strip().lower() for word in value.split(';')]
    for word in marker_words:
        if ' ' in word:
            words_two += [word.split(' ')]
        elif '_' in word:
            words_two += [word.split('_')]
    
    return words_two


def collect_labels(dct_labels, n):
    labels = {k:v for k, v in dct_labels.items() if v > n}
    if bool(labels):
        return labels
    else:
        return {}


def classify_by_topic(text, dict_markers):
    # TRY FIND ONE OF PATTERNS
    try:
        patterns = ['<replaced>']
        for pattern in patterns:
            search = re.search(pattern, text, flags=re.IGNORECASE)

            # IF PATTERN FOUND -> TRY CLASSIFY
            if search:
                search = search[0].split(':')[1]
                search = clear_text(search)
                tokens = lemmatize(search)

                # RETURN
                labels = classify_by_set(tokens, dict_markers)
                if bool(labels):
                    labels = max(labels, key=labels.get)
                    return labels
    except:
        return


def classify_by_set(tokens, dict_markers):
    # DECLARE
    dict_matches = {}

    # ITERATE OVER MARKERS
    for key, value in dict_markers.items():
        dict_matches[key] = 0
        marker_words = collect_marker_words(value)
        marker_words_two = collect_marker_words_two(value)
                
        # FIND MATCHES
        cross = len(set(marker_words)&set(tokens))
        if cross >= 2:
            dict_matches[key] += cross

        for phrase in marker_words_two:
            cross = len(set(phrase)&set(tokens))
            dict_matches[key] += cross
    
    # RETURN
    labels = collect_labels(dict_matches, 1)
    return labels
    
    
def classify_by_simple(tokens, dict_markers):
    # DECLARE
    dict_matches = {}

    # ITERATE OVER MARKERS
    for key, value in dict_markers.items():
        dict_matches[key] = 0
        marker_words = collect_marker_words(value)
        marker_words_two = collect_marker_words_two(value)
        marker_words += [word for lst in marker_words_two for word in lst]

        # CALCULATE
        priority = 3 if len(marker_words)==1 else 1
        for word in marker_words:
             dict_matches[key] += tokens.count(word)*priority

    # RETURN
    labels = collect_labels(dict_matches, 1)
    return labels


def classify_by_phrase(tokens, dict_markers):
    # DECLARE
    dict_matches = {}

    # ITERATE OVER MARKERS
    for key, value in dict_markers.items():
        marker_phrases = []
        dict_matches[key] = 0
        marker_words = collect_marker_words(value)
        marker_words_two = collect_marker_words_two(value)

        # MAKE COMBINATIONS FROM MARKER WORDS
        for subset in itertools.combinations(marker_words, 2):
            marker_phrases.append(subset)

        # CHECK WETHER JIRA-ISSUE TEXT CONTAINS COMBINATION
        for phrase in marker_phrases:
            if set(phrase).issubset(tokens):
                dict_matches[key] += 1

        # CHECK WETHER JIRA-ISSUE TEXT CONTAINS GIVEN PHRASES
        for phrase in marker_words_two:
            if set(phrase).issubset(tokens):
                dict_matches[key] += 1
        
    # RETURN
    labels = collect_labels(dict_matches, 0)
    return labels


def classify_by_vector(tokens, dict_markers):
    try:
        # DECLARE
        dict_matches = {}
        Vectorizer = CountVectorizer()
        tokens = Vectorizer.fit_transform(tokens).toarray()

        # ITERATE OVER MARKERS
        for key, value in dict_markers.items():
            marker_words = collect_marker_words(value)
            marker_words_two = collect_marker_words_two(value)
            marker_words += [word for lst in marker_words_two for word in lst]
            
            # CALCULATE PAIRWISE DISTANCE
            marker_words = Vectorizer.transform(marker_words).toarray()
            vector = 1 - pairwise_distances(tokens, marker_words, metric='cosine')
            dict_matches[key] = vector.sum()
                
        # RETURN
        labels = collect_labels(dict_matches, 1)
        return labels
    except:
        return {}

def classify_by_find_phrase(tokens, dict_markers):
    # DECLARE
    dict_matches = {}
    
    # ITERATE OVER MARKERS
    for key, value in dict_markers.items():
        dict_matches[key] = 0
        marker_words_two = collect_marker_words_two(value)

        # IF MARKER WORD IS A PHRASE
        for word in marker_words_two:
            phrase = ' '.join(word)
            tokens_string = ' '.join(tokens)
            
            # CHECK WETHER PHRASE IS A SUBSTRING OF JIRA-ISSUE TEXT
            if tokens_string.find(phrase) != -1:
                dict_matches[key] += 1

    # RETURN    
    labels = collect_labels(dict_matches, 0)
    return labels


def classify_by_twowords(tokens, dict_markers):
    # DECLARE
    dict_matches = {}
    tokens_phrases = []
    
    # MAKE COMBINATIONS OF WORDS FROM JIRA-ISSUE
    for subset in itertools.combinations(tokens, 2):
        tokens_phrases.append(subset)
    
    # ITERATE OVER MARKERS
    for key, value in dict_markers.items():
        dict_matches[key] = 0
        marker_phrases = []
        marker_words = collect_marker_words(value)

        # MAKE COMBINATIONS OF WORDS FROM MARKER WORDS
        for subset in itertools.combinations(marker_words, 2):
            marker_phrases.append(subset)

        # COMPARE COMBINATIONS
        for phrase in marker_phrases:
            for token_phrase in tokens_phrases:
                cross = len(set(phrase)&set(token_phrase))
                dict_matches[key] += cross
    
    # RETURN
    labels = collect_labels(dict_matches, 0)
    # return labels
    return {}

# SUM ALL LABELS AND RETURN MAX
def get_max_label(*args):
    keys = [key for dct in args for key in dct]
    labels = {k:sum(dct.get(k, 0) for dct in args) for k in keys}
    if bool(labels):
        labels = max(labels, key=labels.get)
        return labels
    else:
        return '<replaced>'


# NEW GATHERING METHOD
def get_max_label_new(lst1, lst2):
    keys1 = [key for dct in lst1 for key in dct]
    keys2 = [key for dct in lst2 for key in dct]
    labels1 = {k:sum(dct.get(k, 0) for dct in lst1) for k in keys1}
    labels2 = {k:sum(dct.get(k, 0) for dct in lst2) for k in keys2}
    labels = {k:labels1.get(k, 0) + labels2.get(k, 0)/2 for k in set(labels1)|set(labels2)}

    return labels

# CLASSIFY
def label_classify_together_rude(tokens, dict_markers):
    #IF ONLY ONE RESULT
    #FIRST BY_FIND_PHRASE
    by_find_phrase = classify_by_find_phrase(tokens, dict_markers)
    if bool(by_find_phrase) and len(by_find_phrase)==1:
        labels = [*by_find_phrase]
        return ''.join(labels)
    
    #SECOND BY_TWOWRODS
    by_twowords = classify_by_twowords(tokens, dict_markers)
    if bool(by_twowords) and len(by_twowords)==1:
        labels = [*by_twowords]
        return ''.join(labels)

    #THIRD BY_PHRASE
    by_phrase = classify_by_phrase(tokens, dict_markers)
    if bool(by_phrase):
        labels = [*by_phrase]
        return ''.join(labels)  
    
    #IF MANY RESULTS
    by_set = classify_by_set(tokens, dict_markers)
    by_vector = classify_by_vector(tokens, dict_markers)
    by_simple = classify_by_simple(tokens, dict_markers)

    results = [by_find_phrase, by_twowords, by_phrase, by_set, by_vector, by_simple]
    labels = get_max_label(*results)
    return labels


# CLASSIFY
def label_classify(tokens, dict_markers):
    by_find_phrase = classify_by_find_phrase(tokens, dict_markers)
    by_twowords = classify_by_twowords(tokens, dict_markers)
    by_phrase = classify_by_phrase(tokens, dict_markers)
    by_set = classify_by_set(tokens, dict_markers)
    by_vector = classify_by_vector(tokens, dict_markers)
    by_simple = classify_by_simple(tokens, dict_markers)

    names = ['by_find_phrase', 'by_twowords', 'by_phrase', 'by_set', 'by_vector', 'by_simple']
    results = [by_find_phrase, by_twowords, by_phrase, by_set, by_vector, by_simple]
   
    # TEST #
    lst1 = [by_find_phrase, by_twowords, by_phrase, by_set]
    lst2 = [by_vector, by_simple]
    lbl = get_max_label_new(lst1, lst2)
    return lbl, dict(zip(names, results))
    # TEST #


# REMOVE THIS - ???
def save_all(file_read):
    file_all = file_read.split('.')[0] + '_all.xlsx'
    try:
        df1 = pd.read_excel(file_all)
        df2 = pd.read_excel(file_read)
        df = pd.concat([df1, df2], axis=0, ignore_index=True)
        df.to_excel(file_all, index=False)
    except:
        df = pd.read_excel(file_read)
        df.to_excel(file_all, index=False)


def process(project, file_read):
    # READ
    df = pd.read_csv(file_read, sep=';', encoding='utf-8')
    old_cols = df.columns.to_list()
    print(log_time(), '<replaced>')
    
    # LOAD ANSWERS FROM CONFLUENCE
    df_markers = get_project_markers(project, CONF_USER, CONF_PSWD)
    df_markers.to_excel(f'Available answers {project}.xlsx', index=False)
    df_markers['marker'] = df_markers['marker'].apply(lambda x: x.replace(' ', ''))

    # MAKE DICTS FROM DF
    dict_markers = dict(zip(df_markers['marker'].values, df_markers['key_words'].values))
    dict_answers = dict(zip(df_markers['marker'].values, df_markers['<replaced>'].values))
    print(log_time(), '<replaced>', '<replaced>', len(df_markers))

    # PROCESSING
    df['description'] = df['description'].fillna('<replaced>')
    df['Description'] = df['description'].apply(clear_text)
    print(log_time(), '<replaced>')

    # LEMMATIZING
    morph = MorphAnalyzer()
    df['Description'] = df['Description'].apply(lambda x: lemmatize(x, morph))
    print(log_time(), '<replaced>')
    
    # LABELS
    df['By_Topic'] = df['description'].apply(lambda x: classify_by_topic(x, dict_markers))
    df['By_Description'] = df['Description'].apply(lambda x: label_classify_together_rude(x, dict_markers))
    
    # REMOVE THIS AFTER
    df['result'] = df['Description'].apply(lambda x: label_classify(x, dict_markers))
    # REMOVE THIS AFTER

    df['new_label'] = df['By_Topic'].fillna(df['By_Description'])
    df['answer'] = df['new_label'].map(dict_answers)
    print(log_time(), '<replaced>')
    
    # SAVE
    old_cols += ['new_label', 'answer', 'result']
    df = df[old_cols]
    
    file_to_save = file_read.split('.')[0] + '_answer.xlsx'
    df.to_excel(file_to_save, index=False)
    save_all(file_to_save)
    print(log_time(), 'Files with answers saved.')


def update(jira, file_to_save):
    # READ
    file = file_to_save.split('.')[0] + '_answer.xlsx'
    df = pd.read_excel(file)
    df['answer'] = df['answer'].fillna('<replaced>')
    
    # UPDATE
    for ind, row in df.iterrows():
        
        # GET ISSUE
        key = row['key']
        issue = jira.issue(key)
        
        # UPDATE LABELS AND PROPOSE ANSWER
        labels = eval(row['labels']) + [row['new_label'], '<replaced>']
        answer = row['answer']
        try:
            fields = {'labels': labels, 'customfield_11400':answer}
            issue.update(fields=fields)
        except:
            print(log_time(), f'Wrong fields: {fields}')


def main(jira, project):
    # LOAD
    file_to_save = f'jira_{project}.csv'
    n_issues = load_issues(jira, project, 10**3, file_to_save)

    # CHECK NUMBER OF ISSUES
    if n_issues == 0:
        print(log_time(), f'{project}: No issues to update.')
    else:
        process(project, file_to_save)
        update(jira, file_to_save)
        print(log_time(), f'{project}: Issues updated.')


if __name__ == '__main__':
    main(JIRA, 'SZN2')
    main(JIRA, 'PRR')
    
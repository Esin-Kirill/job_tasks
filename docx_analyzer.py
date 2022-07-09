#!/usr/bin/env python
# coding: utf-8

from pymorphy2 import MorphAnalyzer
from cgitb import text
from tqdm import tqdm
import time
import docx2txt
import zipfile
import docx
import re
import os


# PATH
path = r'<replaced>' 
os.chdir(path)

# GLOBALS
FILE = '<replaced>.docx'
FILE_XML = os.getcwd() + '/DOC/word/document.xml' 
CATALOG = os.getcwd() + '/DOC'
NOUN_COLOR = '<w:color w:val="FF0000"/>'
VERB_COLOR = '<w:color w:val="00B050"/>'

# FUNSTIONS
def log_time():
    return time.strftime('%H:%M:%S |')


def read_docx(file):
    text = docx2txt.process(file)
    return text


def tag_text(text):
    analyzer = MorphAnalyzer()
    text = clear_text(text)
    nouns, verbs = analyze_text(text, analyzer)
    
    # Словарь => {часть_речи:[список, слов]}
    colors = {'NOUN':nouns, 
              'VERB':verbs}
    return colors


def clear_text(text):    
    if text and type(text)==str:    
        text = re.sub(r"\W+", ' ', text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text, flags=re.IGNORECASE)
        return text
    else:
        return ''


def analyze_text(text, analyzer):
    words = text.split(' ')
    nouns = [word for word in words if 'NOUN' in analyzer.parse(word.strip())[0].tag]
    verbs = [word for word in words if 'VERB' in analyzer.parse(word.strip())[0].tag or 'INFN' in analyzer.parse(word.strip())[0].tag]        
    return nouns, verbs


def extract_content(file):
    # Unzip docx
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(os.getcwd() + '/DOC')
        
    # Get file content
    with open(FILE_XML, 'r', encoding='utf-8') as f:
        content = f.read()

    return content


def tag_content(content, dict_tagged):
    header, content = content.split('<w:body>', 1)
    runs_new = []
    runs = content.split('</w:r>')

    for run in tqdm(runs):
        
        words_new = {'NOUN':[], 'VERB':[]}
        run_new = run
        
        for word in set(dict_tagged["NOUN"]):
            if re.search(fr'{word}\W', run_new):
                words_new["NOUN"] += [word]
                
        for word in set(dict_tagged["VERB"]):
            if re.search(fr'{word}\W', run_new):
                words_new["VERB"] += [word]
        
        if bool(words_new['NOUN']) or bool(words_new['VERB']):
            try:
                run_new = create_new_run(run, words_new)
                runs_new.append(run_new)
            except Exception as err:
                runs_new.append(run)
        else:
            runs_new.append(run)  

    content = header + '<w:body>' + '</w:r>'.join(runs_new)
    content = content.replace('<w:t>', '<w:t xml:space="preserve">')
    return content
    

def create_new_run(run, dict_words):
    # Find split
    nouns = dict_words["NOUN"]
    verbs = dict_words["VERB"]
    
    splt = '<w:t>' if '<w:t>' in run else '<w:t xml:space="preserve">'
    text_new = ''
    text = re.search(fr'{splt}.*</w:t>', run).group(0)
    words = text.replace(splt, '').replace('</w:t>', '').strip().split()    
    
    flag_noun = 0
    flag_verb = 0

    # Check if all text contains words to mark then all text becomes marked
    if len(nouns) == len(words):
        flag_noun = 1
        
    if len(verbs) == len(words):
        flag_verb = 1
        
    if any([flag_noun, flag_verb]):
        color = NOUN_COLOR if flag_noun else VERB_COLOR
        
        if f'</w:rPr>{splt}' in run:
            run = run.split(f'</w:rPr>{splt}')
            run = run[0] + color + f'</w:rPr>{splt}' + run[1]
        else:
            run = run.split(splt)
            run = run[0] + f'<w:rPr>{color}</w:rPr>' + splt + run[1]  
        return run
    
    # Another part
    else:
        tags_zer = '' if len(run.split('<w:r>')) == 1 else run.split('<w:r>')[0]
        tags_beg = re.search(fr'<w:r>.*{splt}', run).group(0)
        for word in words:
            flag = False
            
            if any([re.search(noun, word) for noun in nouns]):
                flag, color = True, NOUN_COLOR
            elif any([re.search(verb, word) for verb in verbs]):
                flag, color = True, VERB_COLOR
            
            if flag:
                word = f'<w:r><w:rPr>{color}</w:rPr><w:t>{word} </w:t></w:r>'
                if not text_new.endswith('</w:t></w:r>'):
                    text_new += '</w:t></w:r>' + word  
                else:
                    text_new += word
            else:
                if text_new.endswith('</w:r>'):
                    text_new += f'{tags_beg}{word} '
                else:
                    text_new += f'{word} '
        
        text_new = re.sub(r'\s+', ' ', text_new)
        text_new = text_new if text_new.endswith('</w:t></w:r>') else text_new + '</w:t></w:r>'
        run = tags_zer + tags_beg + text_new[:-6]
        run = run.replace('<w:t>', '<w:t xml:space="preserve">')
        return run

def save_document(content):   
    # Overwrite content
    content = bytes(content, 'utf-8')
    with open(FILE_XML, 'wb') as f:
        f.write(content)
    
    # Save folder as docx
    document = docx.Document(CATALOG)
    
    file_new = "MARKED_" + FILE
    document.save(file_new)
    return file_new


def main(file):
    # Collect all text
    text = read_docx(file)
    print(log_time(), 'Doc read.')
    
    # Detect part of speech
    dict_tagged = tag_text(text)
    print(log_time(), 'Parts of speech detected.')

    # Extract content
    content = extract_content(file)
    print(log_time(), 'Got XML-content.')

    # Mark content
    content = tag_content(content, dict_tagged)
    print(log_time(), 'Parts of speech highlited.')

    # Save marked file
    file_name = save_document(content)
    print(log_time(), f'Marked doc saved as: {file_name}.')

if __name__ == '__main__':
    main(FILE)

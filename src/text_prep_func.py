# -*- coding: utf-8 -*-
"""
@author: nikshah

text prep functions
"""


import nltk
import string
import html
from nltk.corpus import wordnet
import re


def multipleReplace(text, wordDict):
    """
    take a text and replace words that match the key in a dictionary
    with the associated value, return the changed text
    """
    for key in wordDict:
        text = text.replace(key, wordDict[key])
    return text


def tokenize_text(text):
    """

    :param text: call transcript text
    :return: tokenized version of text
    """
    tokens = nltk.word_tokenize(text) 
    tokens = [token.strip() for token in tokens]
    return tokens


def expand_contractions(text, contraction_mapping):
    """

    :param text: call transcript text
    :param contraction_mapping: map of contractions to expand like i'am we'll etc.
    :return: Expanded contractions like i'am --> i am, we'll --> we will
    """

    contractions_pattern = re.compile('({})'.format('|'.join(contraction_mapping.keys())), 
                                      flags=re.IGNORECASE|re.DOTALL)
    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
    ...
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text

     
def nltk2wn_tag(nltk_tag):
    """
    :param nltk_tag: tag of sentence
    :return: POS tag for each word
    """
    if nltk_tag.startswith('J'):
        return wordnet.ADJ
    elif nltk_tag.startswith('V'):
        return wordnet.VERB
    elif nltk_tag.startswith('N'):
        return wordnet.NOUN
    elif nltk_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None


# lemmatize text based on POS tags  
def lemmatize_text(sentence):
    """
    :param sentence: call transcript text
    :return: sentence with all words converted to root form
    """
    wn_tagged = map(lambda x: (x[0], nltk2wn_tag(x[1])), nltk_tagged)
    res_words = []
    for word, tag in wn_tagged:
      ...
    return " ".join(res_words)


def remove_special_characters(text):
    """
    :param text: call transcript text
    :return: call transcripts without special characters
    """
    tokens = tokenize_text(text)
    pattern = re.compile('[{}]'.format(re.escape(string.punctuation)))
    filtered_tokens = filter(None, [pattern.sub(' ', token) for token in tokens])
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text


def remove_stopwords(text):
    """
    :param text: call transcripts text
    :return: call transcript text after removing stopwords
    """
    tokens = tokenize_text(text)
    filtered_tokens = [token for token in tokens if token not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text


def unescape_html(parser, text):
    return html.unescape(text)


def normalize_corpus(corpus, lemmatize=True, tokenize=False):
    """
    :param corpus: List of call transcript text
    :param lemmatize: If lemmetization is wanted or not
    :param tokenize: If tokenization is wanted or not
    :return: normalized transcript
    """

    normalized_corpus = []  
    for text in corpus:
        text = html.unescape(text)
        text = expand_contractions(text, CONTRACTION_MAP)
        if lemmatize:
            text = lemmatize_text(text)
        else:
            text = text.lower()
        text = remove_special_characters(text)
        text = remove_stopwords(text)
        if tokenize:
            text = tokenize_text(text)
            normalized_corpus.append(text)
        else:
            normalized_corpus.append(text)

    return normalized_corpus


def parse_document(document):
    document = re.sub('\n', ' ', document)
    if isinstance(document, str):
        ...
    sentences = [sentence.strip() for sentence in sentences]
    return sentences        


def clean_text(df):
    """
    :param df: Dask Dataframe
    :return: dask dataframe implementation of normalization of text
    """
    df['cleaned_text'] = df.Text.map(html.unescape)\
        .map(lambda Text: expand_contractions(Text, CONTRACTION_MAP))\
        .map(lemmatize_text)\
        .map(remove_special_characters)\
        .map(remove_stopwords)
    return df



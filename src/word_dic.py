# -*- coding: utf-8 -*-
"""
@author: nikshah
word dic
"""
import nltk

wordDic = {
    ' e-mail ': ' email ',
    ' car car ': ' vehicle vehicle ',
    ' car ': ' vehicle ',
    ' cars ': ' vehicle ',
    ' car`s ': ' vehicle ',
    ' paid in full ': ' paidinfull '
}

CONTRACTION_MAP = {
    "ain`t": "is not",
    "aren`t": "are not",
    "can`t": "cannot",
    "can`t`ve": "cannot have",
    "`cause": "because",
    "could`ve": "could have",
    "couldn`t": "could not",
    "couldn`t`ve": "could not have",
    "didn`t": "did not"
}

stopword_list = set(nltk.corpus.stopwords.words('english'))

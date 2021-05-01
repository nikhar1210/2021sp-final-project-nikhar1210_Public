# -*- coding: utf-8 -*-
"""
@author: nikshah
luigi main
"""


import luigi
from luigi import util
import vertica_python
from luigi import Task, ExternalTask
from luigi.contrib.external_program import ExternalProgramTask
from luigi import LocalTarget
import dask.dataframe as ddf
from dask.distributed import Client
import joblib
from text_prep_func import clean_text
# Sklearn
import pickle
from scipy import sparse

# Plotting tools
import pyLDAvis


class data_load(ExternalProgramTask):
    """
    This function is a luigi external program task which loads data from API and from our internal databases
    requires: Three tasks one for API load and 2 for loading data from the internal databases
    output: parquet file for the blended data from all three sources
    """
    def output(self):
        return LocalTarget('data_download.parquet')

    def program_args(self):
        return('python', '')


    def run(self):

        conn_info={'host':'safsand',
                   'port':5433,
                   'user':'nikhar',
                   'password':'12345',
                   'database':'MBFS',
                   'read_timeout':600,
                   'unicode_error':'ignore',
                   'ssl':False}
        connection=vertica_python.connect(**conn_info)
        cur=connection.cursor()
        cur.execute(""" select * from abc""")
        # .....
        connection.close()
        # ....
        ddf.to_parquet(self.output().path)


@luigi.util.requires(data_load)
class concat_transcript(ExternalTask):
    """
    This class is a luigi task that does some prep work with loaded data and makes it ready for normalization
    requires: blended parquet file from data load task
    output: cleaned parquet file
    """

    def output(self):
        return LocalTarget('transcript_ddf.parquet')

    def run(self):

        full_df = ddf.read_parquet(self.input().path)

        ##### .....

        ddf.to_parquet(self.output().path)


@luigi.util.requires(concat_transcript)
class dask_norm(Task):
    """
    This is a luigi task which kicks off a local cluster and normalizes transcripts
    """

    def output(self):
        return LocalTarget('norm_transcript.parquet')

    def run(self):

        norm_ls = ddf.read_parquet(self.input().path)

        # ...

        client = Client(n_workers=8, memory_limit='4GB')

        # ...

        result = dask_dataframe.map_partitions(clean_text, meta=norm_ls)


        df_w_dask = result.compute()

        client.close()

        #  ...

        norm_df.to_parquet(self.output().path)


@luigi.util.requires(data_load, dask_norm)
class filter_unqualified_call(Task):
    """
    This task based on some internal logic that filters out unqualified calls
    """
    def output(self):
        return LocalTarget('norm_corpus_df.parquet')

    def run(self):

        raw_data = ddf.read_parquet(self.input()['raw_data'].path)
        norm_corp = ddf.read_parquet(self.input()['norm_corp'].path)

        # ...

        norm_corp_ddf.to_parquet(self.output().path)


@luigi.util.requires(filter_unqualified_call)
class word_matrix(Task):
    """
    This task creates topic word matrix
    """

    def output(self):
        return LocalTarget("topicmatrix.npz")

    def run(self):

        import_normalized_df = ddf.read_parquet(self.input().path)

        # ...

        X = vectorizer.fit_transform(documents)
        pickle.dump(vectorizer,open('vectorizer_luigi.pkl','wb'))

        sparse.save_npz(self.output().path, X)


@luigi.util.requires(dask_norm, word_matrix)
class model_train(Task):
    """
    This task train topic model and generates and store out in a pickle file
    """
    def output(self):
        return LocalTarget("luigi_train_model_output.pkl")

    def run(self):

        X = sparse.load_npz(self.input().path)

        client = Client(n_workers=8, memory_limit='4GB')

        with joblib.parallel_backend('dask'):
            lda_output = lda_model.fit_transform(X)

        client.close()

        pickle.dump(lda_output,open(self.output().path,'wb'))


@luigi.util.requires(model_train, dask_norm, word_matrix)
class model_viz(Task):
    """
    This task generates model vizualization
    """

    def output(self):
        return LocalTarget('luigi_model_viz.html')

    def run(self):

        lda_output = pickle.load(open(self.input().path,'rb'))

        panel = pyLDAvis.sklearn.prepare(lda_model, X, vectorizer, mds='tsne',  sort_topics = False)

        pyLDAvis.save_html(panel, self.output().path)


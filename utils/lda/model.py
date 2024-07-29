# gensim
from gensim.models import CoherenceModel
from gensim.models.ldamodel import LdaModel
from gensim.models.ldamulticore import LdaMulticore
from gensim.corpora.dictionary import Dictionary
from gensim.test.utils import datapath
from tqdm import tqdm
from config import gensim_log

# utils
from datetime import datetime
import logging
from utils.processing import preprocess
import os
import plotly.graph_objects as go


# dashboards
import pyLDAvis
import pyLDAvis.gensim
import matplotlib.pyplot as plt

# TSNE dependencies
from sklearn.manifold import TSNE
from bokeh.plotting import figure, output_file, show
from bokeh.models import Label
from bokeh.io import output_notebook
import numpy as np
import matplotlib.colors as mcolors

import pandas as pd
import re


# utils
matcher = re.compile(r"(-*\d+\.\d+) per-word .* (\d+\.\d+) perplexity")


def parse_logfile(path_log):
    likelihoods = []
    with open(path_log, "w") as source:
        for line in source:
            match = matcher.search(line)
            if match:
                likelihoods.append(float(match.group(1)))
    return likelihoods


class LDATopicModeling:

    def __init__(
        self,
        df,
        decade=1960,
        directory="/kaggle/working/models/",
        existing=False,
        n_topics=10,
        worker_nodes=None,
        lang_preprocess=preprocess,
        cross_valid=False,
        epochs=30,
    ):
        # Apply preprocessing on decade data
        self.__documents = df.loc[df.decade == decade, "lyrics"].apply(lang_preprocess)

        # Create a corpus from a list of texts
        self.__id2word = Dictionary(self.__documents.tolist())
        self.__corpus = [
            self.__id2word.doc2bow(doc) for doc in self.__documents.tolist()
        ]

        # training
        if os.path.isfile(existing):
            # Load a potentially pretrained model from disk.
            self.model = LdaModel.load(temp_file)
            self.__cv_results = None  # no cross_valid
            self.__n_topics = n_topics
        elif not cross_valid:
            self.model = LdaMulticore(
                corpus=tqdm(self.__corpus),
                id2word=self.__id2word,
                num_topics=n_topics,
                workers=worker_nodes,
                passes=epochs,
            )
            self.__likelihood = parse_logfile(gensim_log)
            self.__n_topics = n_topics
            self.__cv_results = None
        else:  # cross validation

            # hyperparameter
            alpha = []  # np.arange(0.01, 1, 0.3).tolist()
            alpha.append("symmetric")
            alpha.append("asymmetric")

            # hyperparameter
            eta = []  # np.arange(0.01, 1, 0.3).tolist()
            eta.append("symmetric")

            # compute results of the cross_validation
            cv_results = {"topics": [], "alpha": [], "eta": [], "coherence": []}

            # topic range
            topic_range = range(2, n_topics + 1)

            # prevent the computation time
            total = len(eta) * len(alpha) * len(topic_range)
            print("total lda computation: ", total)
            model_list = []

            for k in topic_range:
                for a in alpha:
                    for e in eta:

                        # train the model
                        model = LdaMulticore(
                            corpus=self.__corpus,
                            id2word=self.__id2word,
                            num_topics=k,
                            workers=worker_nodes,
                            passes=epochs,
                            alpha=a,
                            eta=e,
                        )

                        # compute coherence
                        cv = CoherenceModel(
                            model=model,
                            texts=self.__documents,
                            dictionary=self.__id2word,
                            coherence="c_v",
                        )

                        print(
                            "coherence: {}\nalpha: {}\neta: {}\ntopic: {}".format(
                                cv.get_coherence(), a, e, k
                            )
                        )

                        # Save the model results
                        cv_results["topics"].append(k)
                        cv_results["alpha"].append(a)
                        cv_results["eta"].append(e)
                        cv_results["coherence"].append(cv.get_coherence())
                        model_list.append(model)
            # retrieve index of the highest coherence model
            best_index = np.argmax(cv_results["coherence"])

            # choose the model given the best coherence
            self.model = model_list[best_index]

            # save results as attribute
            self.__cv_results = cv_results

            self.__n_topics = cv_results["topics"][best_index]

            # logging doesn't work on Kaggle
            # self.__likelihood = parse_logfile()
        # directory path
        self.__directory = directory

    # getters
    @property
    def get_id2word(self):
        return self.__id2word

    @property
    def get_corpus(self):
        return self.__corpus

    @property
    def get_likelihood(self):
        return self.__likelihood

    @property
    def get_cv_results(self):
        return pd.DataFrame(self.__cv_results) if self.__cv_results else None

    def plot_coherence(self, metric="alpha"):
        """metric(str): alpha or eta"""
        if self.__cv_results is None:
            raise Exception("No cross validation available")

        # get the dataframe
        df_res = self.get_cv_results

        # groupby by metric
        grouped = df_res.groupby(metric)
        # create the layout
        fig = go.Figure()
        for level, df in grouped:
            fig.add_trace(
                go.Scatter(
                    x=df.topics, y=df.coherence, mode="lines+markers", name=str(level)
                )
            )
        fig.update_layout(
            title="coherence over topics by",
            xaxis_title="topic",
            yaxis_title="coherence",
        )
        return fig

    def save_current_model(self):
        # retrieve time
        now = datetime.now()
        # create the directory if it doesn't exist
        try:
            os.makedirs(directory + now.strftime("%d%m%Y_%H%M%S"))
        except:
            pass
        # Save model to disk.
        temp_file = datapath(directory + now.strftime("%d%m%Y_%H%M%S") + "/model")

        self.model.save(temp_file)

    def get_perplexity(self):
        return self.model.log_perplexity(self.__corpus)

    def get_coherence(self):
        coherence_model_lda = CoherenceModel(
            model=self.model,
            texts=self.__documents,
            dictionary=self.__id2word,
            coherence="c_v",
        )
        return coherence_model_lda.get_coherence()

    # data vizualisation
    def dashboard_LDAvis(self):
        # some basic dataviz
        pyLDAvis.enable_notebook()
        vis = pyLDAvis.gensim.prepare(
            self.model, self.__corpus, dictionary=self.model.id2word
        )
        return vis

    def plot_tsne(self, components=2):
        # n-1 rows each is a vector with i-1 posisitons, where n the number of documents
        # i the topic number and tmp[i] = probability of topic i
        topic_weights = []
        for row_list in self.model[self.get_corpus]:
            tmp = np.zeros(self.__n_topics)
            for i, w in row_list:
                tmp[i] = w
            topic_weights.append(tmp)

        # Array of topic weights
        arr = pd.DataFrame(topic_weights).fillna(0).values

        # Keep the well separated points
        # filter documents with highest topic probability given lower bown (optional)
        # arr = arr[np.amax(arr, axis=1) > 0.35]

        # Dominant topic number in each doc (to compute color)
        topic_num = np.argmax(arr, axis=1)

        # tSNE Dimension Reduction
        tsne_model = TSNE(n_components=components, verbose=1, init="pca")
        tsne_lda = tsne_model.fit_transform(arr)

        mycolors = np.array([color for name, color in mcolors.TABLEAU_COLORS.items()])

        # components
        if components == 2:
            fig = go.Figure(
                go.Scatter(
                    x=tsne_lda[:, 0],
                    y=tsne_lda[:, 1],
                    marker_color=mycolors[topic_num],
                    mode="markers",
                    name="Tsne",
                )
            )
            fig.update_layout(
                title="t-SNE Clustering of {} LDA Topics".format(self.__n_topics),
                xaxis_title="x",
                yaxis_title="y",
                autosize=False,
                width=900,
                height=700,
            )
        elif components == 3:
            fig = go.Figure(
                go.Scatter3d(
                    x=tsne_lda[:, 0],
                    y=tsne_lda[:, 1],
                    z=tsne_lda[:, 2],
                    marker_color=mycolors[topic_num],
                    mode="markers",
                    name="Tsne",
                )
            )
            fig.update_layout(
                title="t-SNE Clustering of {} LDA Topics".format(self.__n_topics),
                xaxis_title="x",
                yaxis_title="y",
            )
        else:
            raise Exception("Components exceed covered numbers : {}".format(components))
        return fig

        # plot = figure(title="t-SNE Clustering of {} LDA Topics".format(self.__n_topics),
        #             plot_width=900, plot_height=700)
        # plot.scatter(x=tsne_lda[:,0], y=tsne_lda[:,1], color=mycolors[topic_num])
        # show(plot)

    def plot_likelihood(self):
        fig = go.Figure(
            go.Scatter(
                x=[i for i in range(0, 50)],
                y=self.__likelihood[-50:],
                mode="lines",
                name="lines",
            )
        )
        fig.update_layout(
            title="Likelihood over passes",
            xaxis_title="Likekihood",
            yaxis_title="passes",
        )
        return fig

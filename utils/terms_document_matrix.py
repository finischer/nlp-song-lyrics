from tqdm import tqdm
from sklearn.feature_extraction.text import CountVectorizer
from utils.processing import preprocess
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go


class TermsDocumentsMatrix:

    def __init__(self, sdf, decades=[1960, 1970], colorscale="Plotly3"):
        # vectorizer on the sample lyrics
        self.__vectorizer = CountVectorizer(tokenizer=preprocess)
        # fit and transform the data
        self.__data_vectorized = self.__vectorizer.fit_transform(
            tqdm(sdf["lyrics"].loc[sdf["decade"].isin(decades)])
        )
        # get decades informations
        self.__decades = (
            sdf["decade"].loc[sdf["decade"].isin(decades)].reset_index(drop=True)
        )
        self.__unique_decades = decades
        # get colorscale template
        self.__colorscale = colorscale

    def get_tdmatrix(self):

        # compute a Matrix terms document by decades
        df_bw = pd.DataFrame(
            self.__data_vectorized.toarray(),
            columns=self.__vectorizer.get_feature_names_out(),
        )

        # check the length
        if len(df_bw) != len(self.__decades):
            raise Exception("Not the same size")

        # concatenate decade
        df_bw["decade"] = self.__decades

        # check NaN values
        if len(df_bw.columns[df_bw.isna().any()].tolist()) != 0:
            raise Exception("Decade got Nan values")

        return df_bw

    def get_tdm_by_decade(self, decade):

        if decade not in self.__unique_decades:
            raise Exception("{} doesn't appear in the decades list".format(decade))

        # compute a Matrix terms document by decades (bag of words format)
        df_bw = pd.DataFrame(
            self.__data_vectorized.toarray(),
            columns=self.__vectorizer.get_feature_names_out(),
        )

        # check the length
        if len(df_bw) != len(self.__decades):
            raise Exception("Not the same size")

        # concatenate decade
        df_bw["decade"] = self.__decades

        # check NaN values
        if len(df_bw.columns[df_bw.isna().any()].tolist()) != 0:
            raise Exception("Decade got Nan values")

        # select suitable decade
        df_bw = df_bw[df_bw["decade"] == decade]

        return df_bw

    def most_freq_terms(self, n_rows=1, n_cols=2, n_terms=10):

        # create the document terms matrix
        df_bw = self.get_tdmatrix()

        # create en make subplot
        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            x_title="number of occurrences",
            y_title="terms",
            subplot_titles=self.__unique_decades,
        )

        for i in range(0, n_rows):
            for k in range(0, n_cols):
                if (i * n_rows + k) == len(self.__unique_decades):
                    break

                # get the decade
                decade = self.__unique_decades[i * n_rows + k]

                # select the suitable decade and delete decade column
                df_decade = df_bw.loc[df_bw.decade == decade, df_bw.columns != "decade"]

                # compute terms frequencies by decade
                terms_freq = df_decade.sum().sort_values(ascending=False)

                # total number of terms occurences
                total_terms = terms_freq.values

                # add figure
                fig.add_trace(
                    go.Bar(
                        y=terms_freq.index.tolist()[:n_terms][::-1],
                        x=total_terms[:n_terms][::-1],
                        name=decade,
                        orientation="h",
                        showlegend=False,
                        marker=dict(color=total_terms, colorscale=self.__colorscale),
                    ),
                    i + 1,
                    k + 1,
                )
        return fig

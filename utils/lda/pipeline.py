# LDA Topic Modeling by decade

from utils.lda.model import LDATopicModeling
from utils.processing import preprocess
import pandas as pd


class LDAPipeline:

    def __init__(
        self,
        prep=preprocess,
        cv=False,
        decades=[1960, 1970, 1980, 1990, 2000, 2010, 2020],
    ):
        self.models = {
            decade: LDATopicModeling(
                decade=decade, lang_preprocess=prep, epochs=10, cross_valid=cv
            )
            for decade in decades
        }

    def get_metrics(self):
        # compute metrics
        metrics = {"decade": [], "coherence": [], "perplexity": []}
        for decade, model in self.models.items():
            metrics["decade"].append(decade)
            metrics["coherence"].append(model.get_coherence())
            metrics["perplexity"].append(model.get_perplexity())
        # create the dataframe
        df_m = pd.DataFrame(metrics)
        df_m.set_index("decade")
        return df_m

    def lda_info(self, decade):
        lda_model = self.models[decade]

        print("Perplexity: ", lda_model.get_perplexity())
        print("Coherence: ", lda_model.get_coherence())
        lda_model.plot_tsne()
        return lda_model.dashboard_LDAvis()

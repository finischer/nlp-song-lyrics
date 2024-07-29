# default preprocessing
import spacy

print("set gpu: ", spacy.prefer_gpu())

# small model /!\ take the bigger one for Kaggle
new_nlp = spacy.load("en_core_web_sm")


def preprocess(text, nlp=new_nlp):

    # TOKENISATION
    tokens = []
    for token in nlp(text):
        tokens.append(token)

    # REMOVING STOP WORDS
    spacy_stopwords = nlp.Defaults.stop_words
    sentence = [
        word
        for word in tokens
        if word.text.isalpha() and word.text not in spacy_stopwords
    ]

    # LEMMATISATION
    sentence = [word.lemma_ for word in sentence]

    return sentence

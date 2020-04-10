import pandas as pd
from nltk import WordNetLemmatizer


def lemma_df(df: pd.DataFrame(columns=["Plot"])
             ) -> pd.DataFrame(columns=["Plot"]):
    """
    df lemmatisation

    """
    df['Plot'] = df["Plot"].apply(
        lambda row: [WordNetLemmatizer().lemmatize(sym)
                     for sym in row if sym != " "])
    return df


def tokenize_df(df: pd.DataFrame(columns=["Plot"])
                ) -> pd.DataFrame(columns=["Plot"]):
    """
    df tokenization

    """
    df['Plot'].replace(r"[^\w\s]", " ", regex=True, inplace=True)  # Removes all punctuations
    df['Plot'].replace(r"\d+", " ", regex=True, inplace=True)  # Replaces digits
    df['Plot'].replace(r"\b[a-zA-Z]\b", " ", regex=True, inplace=True)  # Removes all single characters
    # df['Plot'] = df["Plot"].apply(lambda row: word_tokenize(row)) alternative
    df["Plot"] = df["Plot"].astype(str).str.lower()
    df["Plot"] = df["Plot"].apply(lambda text: text.split(" "))
    return df

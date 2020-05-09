import numpy as np
import pandas as pd
import torch
import transformers as ppb

from constants import DATA_PATH, BERT_DATA_PATH, MAX_LEN_BERT
from helper_funcs import create_directories


def build_embeddings() -> int:
    """

    Build embeddings for each document in collection using DistilBERT, store as csv files in BERT_PATH folder
    """
    model, tokenizer = get_model_and_tokenizer()

    file_count: int = 0  # for naming and merge
    for df in pd.read_csv(DATA_PATH,
                          usecols=["Title", "Plot"],
                          chunksize=100  # otherwise too slow or buy memory
                          ):
        df['Plot tokenized'] = df['Plot'].apply((lambda row: tokenizer.encode(row,
                                                                              add_special_tokens=True,
                                                                              max_length=MAX_LEN_BERT)))
        # prepare input for BERT
        padded = np.array([i + [0] * (MAX_LEN_BERT - len(i)) for i in df['Plot tokenized'].values])
        features = get_embeddings_processed_by_bert(model, padded)
        df['BERT output'] = features
        df.to_csv(BERT_DATA_PATH + f'data_bert{file_count}.csv')
        file_count = file_count + 1

    return file_count


def get_embeddings_processed_by_bert(model: ppb.DistilBertModel, padded: ppb.DistilBertTokenizer) -> list:
    """

    Perform model on input padded and nask matrix, get output of DistilBERT embeddings builded
    """
    attention_mask = np.where(padded != 0, 1, 0)  # to BERT ignore padding
    input_ids = torch.LongTensor(padded)
    attention_mask = torch.LongTensor(attention_mask)
    with torch.no_grad():
        last_hidden_states = model(input_ids, attention_mask=attention_mask)
    embeddings = last_hidden_states[0][:, 0, :].numpy().tolist()  # BERT output
    return embeddings


def get_model_and_tokenizer() -> [ppb.DistilBertModel, ppb.DistilBertTokenizer]:
    """

    Get pretraind model and tokenizer DistilBERT
    """
    # For DistilBERT:
    model_class, tokenizer_class, pretrained_weights = (
        ppb.DistilBertModel, ppb.DistilBertTokenizer, 'distilbert-base-uncased')
    # Load pretrained model/tokenizer
    tokenizer = tokenizer_class.from_pretrained(pretrained_weights)
    model = model_class.from_pretrained(pretrained_weights)
    return model, tokenizer


def merge_one_df(file_count: int):
    """

    Merges all files for batches in one result.csv df
    """
    list_of_dfs = [pd.read_csv(BERT_DATA_PATH + f'data_bert{i}.csv') for i in range(file_count)]
    combined_df = pd.concat(list_of_dfs, ignore_index=True)
    combined_df.to_csv(BERT_DATA_PATH + f'result.csv')


if __name__ == "__main__":
    create_directories(BERT_DATA_PATH)
    count = build_embeddings()
    merge_one_df(count)

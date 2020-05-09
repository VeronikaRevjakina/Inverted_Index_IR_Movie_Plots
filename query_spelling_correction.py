from deeppavlov import build_model, configs

CONFIG_PATH = configs.spelling_correction.brillmoore_wikitypos_en
model = build_model(CONFIG_PATH, download=True)


def get_spelling_corrected_query(query_in: str) -> str:
    """

   Spelling correction for query with deevpavlov spelling_correction.brillmoore_wikitypos_en
    """
    return model([query_in])[0]



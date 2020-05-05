from deeppavlov import build_model, configs
CONFIG_PATH = configs.spelling_correction.brillmoore_wikitypos_en
model = build_model(CONFIG_PATH, download=True)

if __name__ == "__main__":
    query = input('Enter your query:')
    print(model([query])[0], flush=True)

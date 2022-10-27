import pandas as pd

def create_data(entry_id, connection):
    data = pd.read_sql(f"SELECT * FROM split_sentences where entry_id = {entry_id}", con=connection)
    data['sentence'] = data['sentence'].str.replace(r'<[^<>]*>', '', regex=True)
    return data

def get_versions(data):
    return set(data.version)

def get_text(data, version):
    versions = set(data.version)
    if version in versions:
        s = ""
        for sentence in data[data.version == version].sentence:
            s += sentence
            
        return s.split('--')[1]
    else:
        return ""
     
def print_version(data, version):
    versions = set(data.version)
    if version in versions:
        for sentence in data[data.version == version].sentence:
            print(sentence, "\n")
    else:
        print(f"unsupported version: {version} -  available versions: {versions}")
        

def get_documents(data):
    return [get_text(data, version) for version in get_versions(data)]

def doc_level_stats(entry_id, connection):
    return pd.read_sql(f"SELECT * FROM doc_level_stats where entry_id = {entry_id}", con=connection)

def show_tables(connection):
    return (pd.read_sql("""SELECT name FROM sqlite_master WHERE type='table';""", con=connection))

def show_table(name, connection, n=5):
    return (pd.read_sql(f"SELECT * FROM {name} LIMIT {n};", con=connection))

def get_url(entry_id, connection):
    df = pd.read_sql(f"SELECT * FROM entry WHERE id = {entry_id};", con=connection)
    return df.url
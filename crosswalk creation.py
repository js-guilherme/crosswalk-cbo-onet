import os
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from datetime import datetime

model = SentenceTransformer("Alibaba-NLP/gte-multilingual-base", trust_remote_code=True)

data_cbo = pd.read_excel("cbo description.xlsx", sheet_name=0)
data_onet = pd.read_excel(r"onet description.xlsx")

embeddings_cbo_titles = model.encode(data_cbo['Title'].tolist())
embeddings_cbo_descriptions = model.encode(data_cbo['Description'].tolist())

score_onet_cbo = []
match_list = []

for i in range(len(data_onet)):
    if (i + 1) % 100 == 0:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{current_time} - Lendo registro {i + 1}")

    embeddings_onet_titles = model.encode([data_onet.iloc[i]['Title']])
    embeddings_onet_descriptions = model.encode([data_onet.iloc[i]['Description']])

    similarity_scores_title = util.cos_sim(embeddings_cbo_titles, embeddings_onet_titles).squeeze().tolist()
    similarity_scores_description = util.cos_sim(embeddings_cbo_descriptions, embeddings_onet_descriptions).squeeze().tolist()

    for j in range(len(data_cbo)):
        score_onet_cbo.append({
            'code_onet': data_onet.iloc[i]['Code'],
            'title_onet': data_onet.iloc[i]['Title'],
            'code_cbo': data_cbo.iloc[j]['Code'],
            'desc_onet': data_onet.iloc[i]['Description'],
            'title_cbo': data_cbo.iloc[j]['Title'],
            'description_cbo': data_cbo.iloc[j]['Description'],
            'score_title': similarity_scores_title[j],
            'score_description': similarity_scores_description[j],
            'jbz_onet': data_onet.iloc[i]['Job Zone'],
            'jbz_cbo': data_cbo.iloc[j]['Jobzone']
        })

    match = pd.DataFrame(score_onet_cbo)
    match['score_total'] = match['score_description'] + match['score_title']
    match = match[match['jbz_onet'] == match['jbz_cbo']]
    
    if not match.empty:
        match = match[match['score_total'] == match['score_total'].max()]
        match_list.append(match)

    score_onet_cbo.clear()

crosswalk1 = pd.concat(match_list, ignore_index=True) if match_list else pd.DataFrame()

data_cbo = data_cbo[~data_cbo['Code'].isin(match['code_cbo'])]

embeddings_onet_titles = model.encode(data_onet['Title'].tolist())
embeddings_onet_descriptions = model.encode(data_onet['Description'].tolist())

score_onet_cbo = []
match_list = []

for i in range(len(data_cbo)):
    if (i + 1) % 100 == 0:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{current_time} - Lendo registro {i + 1}")

    embeddings_title_cbo = model.encode([data_cbo.iloc[i]['Title']])
    embeddings_description_cbo = model.encode([data_cbo.iloc[i]['Description']])

    similarity_scores_title = util.cos_sim(embeddings_title_cbo, embeddings_onet_titles).squeeze().tolist()
    similarity_scores_description = util.cos_sim(embeddings_description_cbo, embeddings_onet_descriptions).squeeze().tolist()

    for j in range(len(data_onet)):
        score_onet_cbo.append({
            'code_onet': data_onet.iloc[j]['Code'],
            'title_onet': data_onet.iloc[j]['Title'],
            'code_cbo': data_cbo.iloc[i]['Code'],
            'desc_onet': data_onet.iloc[j]['Description'],
            'title_cbo': data_cbo.iloc[i]['Title'],
            'description_cbo': data_cbo.iloc[i]['Description'],
            'score_title': similarity_scores_title[j],
            'score_description': similarity_scores_description[j],
            'jbz_onet': data_onet.iloc[j]['Job Zone'],
            'jbz_cbo': data_cbo.iloc[i]['Jobzone']
        })

    match = pd.DataFrame(score_onet_cbo)
    match['score_total'] = match['score_description'] + match['score_title']
    match = match[match['jbz_onet'] == match['jbz_cbo']]
    
    if not match.empty:
        match = match[match['score_total'] == match['score_total'].max()]
        match_list.append(match)

    score_onet_cbo.clear()

crosswalk2 = pd.concat(match_list, ignore_index=True) if match_list else pd.DataFrame()

print("It's Over!")

final_crosswalk = pd.concat([crosswalk1, crosswalk2], ignore_index=True)

final_crosswalk.to_excel("crosswalk4.xlsx", index=False)

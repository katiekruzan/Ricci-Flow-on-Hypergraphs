import requests
import pandas as pd
import time

base_url = "https://api.semanticscholar.org/graph/v1/paper/"
headers = {'X-API-KEY': 'pryS2rPqdiaczCP3oSKPf1jhb0a8tIFd8vFlOKyD'}
MAX_DATA_LIMIT = 10000
BATCH_SIZE = 420


# Function to perform a GET request with retry for rate limits
def req_with_retry(reqType,
                   url,
                   params,
                   body=None,
                   retries=3,
                   backoff_factor=2):
    for attempt in range(retries):
        if reqType == 'POST':
            response = requests.post(url,
                                     params=params,
                                     headers=headers,
                                     json=body)
        else:  # 'GET'
            response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            return response
        elif response.status_code == 429:  # Too Many Requests
            wait_time = backoff_factor * (2**attempt)
            print(f"Rate limit hit. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            response.raise_for_status()
    response.raise_for_status()


# Function to get citing papers
def get_citing_papers(paper_id, citation_count, limit=1000):
    url = f"{base_url}{paper_id}/citations"
    data = []
    print("Getting cited papers for {}".format(paper_id))
    for off in range(0, min(int(citation_count), MAX_DATA_LIMIT - limit + 1),
                     limit):
        print("Reading citations for {} - offset = {}".format(paper_id, off))
        params = {
            'offset': off,
            'limit': limit if off + limit < MAX_DATA_LIMIT else limit - 1
        }
        response = req_with_retry('GET', url, params)
        data = data + response.json()['data']
    return data


# Function to get paper details
def get_paper_details(paper_id):
    url = f"{base_url}{paper_id}"
    params = {'fields': 'paperId,title,citationCount,authors'}
    response = req_with_retry('GET', url, params)
    data = response.json()
    return data


# Function to get papers in batch
# https://api.semanticscholar.org/api-docs/graph#tag/Paper-Data/operation/post_graph_get_papers
def get_paper_batch(ids, fields):
    url = "{}batch".format(base_url)
    params = {'fields': fields}
    body = {
        'ids': ids,
    }
    response = req_with_retry('POST', url, params, body)
    return response.json()


# Function to process each paper
def process_paper(paper_id):
    original_paper = get_paper_details(paper_id)
    print(f"Original Paper: {original_paper['title']}\n\n")

    citing_papers = get_citing_papers(paper_id,
                                      original_paper['citationCount'])

    data_list = []

    print("Read metadata of citing papers")
    for i in range(0, len(citing_papers) + BATCH_SIZE, BATCH_SIZE):
        print(f"Reading citations for {paper_id} - batch index = {i}")
        paper_ids = [
            paper.get('citingPaper').get('paperId')
            for paper in citing_papers[i:i + BATCH_SIZE]
        ]
        paper_deets = get_paper_batch(
            paper_ids,
            'title,authors,citationCount') if len(paper_ids) > 0 else []
        for pdeet in paper_deets:
            if pdeet and pdeet.get('authors'):
                authors = [_.get('name') for _ in pdeet.get('authors')]
                author_ids = [_.get('authorId') for _ in pdeet.get('authors')]
                data_list.append([
                    pdeet['paperId'], pdeet['title'], pdeet['citationCount'],
                    authors, author_ids
                ])

    df = pd.DataFrame(data_list,
                      columns=[
                          'paper_id', 'title', 'citation_count',
                          'author_names', 'author_ids'
                      ])
    df['orignal_paper_id'] = paper_id

    return df


'''
# Paper IDs to check
paper_ids = [
    "d61031326150ba23f90e6587c13d99188209250e",  # Barabasi, A.L., Albert, R.: Emergence of scaling in random networks.
    "2a005868b79511cf8c924cd5990e2497527a0527",  # Girvan, M., Newman, M.E.: Community structure in social and biological networks.
    "525199d30a5a975fb32e7944924c82b584fea1d0"  # Watts, D.J., Strogatz, S.H.: Collective dynamics of small-world networks.
]
'''

# Paper IDs to check
paper_ids = [
    "f9b4a25845d741076f189480143af29f7b195373",  # Hardness vs Randomness
    "9fb53a3bdfb47230eeaf7d956b1a238db5cba690",  # Reducibilty among combinatorial problems
    "683c8f5c60916751bb23f159c86c1f2d4170e43f"  # Probabilistic Encryption
]

# Process data
data = pd.concat([process_paper(id) for id in paper_ids])

# Write dataset
data.to_csv('./dataset_turingaward.csv')
"""
filtered_df = data[data['orignal_paper_id'] == '525199d30a5a975fb32e7944924c82b584fea1d0'].sort_values(by='citation_count', ascending=False)
filtered_df[:100]

"""
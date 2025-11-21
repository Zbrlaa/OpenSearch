# Configuration
import json # Pour lire le fichier JSON
from opensearchpy import OpenSearch # API Python pour OpenSearch

OS_HOST='localhost'
OS_PORT=9200
NEWS_JSON_PATH='/home/user/Documents/OpenSearch/data/News_Category_Dataset_v3.json'
INDEX_NAME='news'
MAX_NEWS=10000 # Nombre maximum d'articles à indexer

# Ingestion des données
def payload_constructor(data):
	'''Coonstruction du payload pour l'indexation en masse'''
	payload_string = ''
	for datum in data:
		action = {'index': {'_id': datum['link']}}
		action_string = json.dumps(action) + '\n'
		payload_string += action_string
		this_line = json.dumps(datum) + '\n'
		payload_string += this_line
	return payload_string


index_name = INDEX_NAME
batch_size = 1000 # Nombre d'articles à indexer à chaque fois

client = OpenSearch(
	hosts=[{'host': 'localhost', 'port': 9200}],
	http_compress=True,
)

with open(NEWS_JSON_PATH) as f:
	lines = f.readlines()

processed_lines = 0
for start in range(0, len(lines), batch_size):
	data = []
	for line in lines[start:start+batch_size]:
		data.append(json.loads(line))
		processed_lines += 1
	response = client.bulk(body=payload_constructor(data), index=index_name)
	print("{} lines processed.".format(processed_lines))
	if processed_lines >= MAX_NEWS:
		print('{} lines reached. Abort indexing'.format(MAX_NEWS))
		break
from pymed import PubMed
import json

pubmed = PubMed(tool="PubmedSearch", email="admin@kaaass.net")

start = '2013/01/01'
end = '2017/01/01'
query = '(("english"[Language]) AND "case reports"[Publication Type]) ' \
    + f'AND ("{start}"[Date - Publication] : "{end}"[Date - Publication]) ' \
    + 'AND ("humans"[MeSH Terms]) AND ("Case Reports"[ptyp]) AND ("English"[lang]) ' \
    + 'AND ("pubmed pmc local"[sb]))'

results = pubmed.query(query, max_results=5000)

count = 0

def save(force=False, every=100):
    global count
    if not force:
        count += 1
        if count >= every:
            count = 0
        else:
            return
    print('Save data, fetched', len(data))
    with open('data.json', 'w') as f:
        json.dump(data, f)

data = []

for article in results:
    pmid = int(article.pubmed_id.split('\n')[0])
    title = article.title
    keywords = []
    if article.keywords:
        if None in article.keywords:
            article.keywords.remove(None)
        keywords = article.keywords
    publication_date = article.publication_date
    abstract = article.abstract

    data.append({
        'pmid': pmid,
        'title': title,
        'keywords': keywords,
        'publication_date': str(publication_date),
        'abstract': abstract
    })

    save()

save(force=True)

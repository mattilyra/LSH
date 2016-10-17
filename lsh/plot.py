import networkx as nx
import matplotlib.pyplot as plt

from lsh.cache import Cache
from lsh.minhash import MinHasher

if __name__ == '__main__':
    data = dict()
    hasher = MinHasher(seeds=200, char_ngram=5, hashbytes=4)
    lsh = Cache(hasher=hasher, band_width=10)

    with open('../newsSummaries/data.csv') as fh:
        for line in fh:
            i, title, content = line.split('\t')
            content = title + ' ' + content
            data[i] = content
            lsh.update(content, i)

    graph = nx.Graph()
    graph.add_edges_from(lsh.get_all_duplicates())
    nx.draw(graph, with_labels=True)
    plt.savefig('test.png', format='png')

    with open('components.txt', 'w') as outf:
        for subgraph in nx.connected_component_subgraphs(graph):
            for node_id in subgraph.nodes():
                outf.write('%s\t%s' % (node_id, data[node_id]))
            outf.write('\n\n')
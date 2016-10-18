import logging
import os
from glob import glob

import matplotlib.pyplot as plt
import networkx as nx

from lsh.cache import Cache
from lsh.minhash import MinHasher

if __name__ == '__main__':  # pragma: no cover
    format = "%(asctime)s\t%(module)s.%(funcName)s (line %(lineno)d)" \
             "\t%(levelname)s : %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)

    for fname in glob('../relevance/ssais*csv'):
        logging.info('-' * 50)
        logging.info('Doing %s' % fname)
        data = dict()
        out_prefix = os.path.splitext(os.path.basename(fname))[0]
        hasher = MinHasher(seeds=100, char_ngram=5, random_state=42)
        lsh = Cache(hasher=hasher, num_bands=10)

        logging.info('Updating cache')
        with open(fname) as fh:
            for line in fh:
                i, title, content = line.split('\t')
                content = title + ' ' + content
                data[i] = content
                lsh.update(content, i)
        logging.info('Updated cache with %d docs', len(lsh.seen_ids))

        graph = nx.Graph()
        logging.info('Getting duplicates')
        graph.add_edges_from(lsh.get_all_duplicates(min_jaccard=0.9,
                                                    data=data))

        with open('%s-components.txt' % out_prefix, 'w') as outf:
            for subgraph in nx.connected_component_subgraphs(graph):
                if len(set(data[node] for node in subgraph.nodes())) == 1:
                    # logging.info('%d identical docs: %r' %
                    #              (len(subgraph.nodes()), subgraph.nodes()))
                    continue

                logging.info('%d near-duplicate docs: %r' %
                             (len(subgraph.nodes()), subgraph.nodes()))
                for node_id in subgraph.nodes():
                    outf.write('%s\t%s\n\n' % (node_id, data[node_id][:500]))
                outf.write('\n-------------------------------------------\n')

        # remove 0-degree nodes (not duplicates)
        outdeg = graph.degree()
        to_keep = [n for n in outdeg if outdeg[n] > 1]
        graph = graph.subgraph(to_keep)

        nx.draw(graph, with_labels=True)
        plt.savefig('%s.png' % out_prefix, format='png')

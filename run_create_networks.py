import json
from networkx import nx

# import dynamicgexf

# TODO: allow the definition of author lists - for coloring the nodes etc.
# See the ICICKM 2013 article for details. That is, the following
# Still, K., Huhtamäki, J., & Russell, M. G. (2013). Relational Capital and
# Social Capital: One or two Fields of Research?
# In Proceedings of the 10th International Conference on Intellectual Capital,
# Knowledge Management and Organisational Learning,
# The George Washington University, Washington, DC, USA, 24-25 October 2013
# (pp. 420–428).

class AuthorNetwork:

  def __init__(self,directed=True):
   self.graph = nx.DiGraph()
   # self.sc_authors = sc_authors
   # self.rc_authors = rc_authors
   if not directed:
     self.graph = nx.Graph()

  def link_authors(self,author_a,author_b,relation,sample):
    if self.graph.has_edge(author_a,author_b):
      self.graph[author_a][author_b]['weight'] += 1
      if self.graph[author_a][author_b]['relation'] != relation:
        self.graph[author_a][author_b]['relation'] = 'multiple'
    else:
      self.graph.add_edge(author_a,author_b,weight=1,relation=relation)
    self.graph.node[author_a][sample] = True
    self.graph.node[author_b][sample] = True

  def metrify(self):
    betweenness = centrality.betweenness_centrality(self.graph)
    for node in betweenness:
      self.graph.node[node]['betweenness'] = betweenness[node]

  def color_nodes(self):
    return

    for node,data in self.graph.nodes(data=True):
      # print 'Setting color for %s' % (node[0])
      print(node,data)
      # node is a tuple representing the node data
      #  Hot pink: 255-105-180
      color = {'r': 255, 'g': 105, 'b': 180, 'a': 0.8}

      self.graph.node[node]['viz'] = {'color': color}

      return

      if node in self.sc_authors and node in self.rc_authors:
      # if data.has_key('relational-capital') and data.has_key('social-capital'):
        color = {'r': 255, 'g': 255, 'b': 0, 'a': 0.8}
      elif node in self.rc_authors:
      # elif data.has_key('relational-capital'):
        color = {'r': 0, 'g': 255, 'b': 0, 'a': 0.8}
      elif node in self.sc_authors:
      # elif data.has_key('social-capital'):
        color = {'r': 255, 'g': 0, 'b': 0, 'a': 0.8}
      self.graph.node[node]['viz'] = {'color': color}

  def serialize(self,outfile):
    self.color_nodes()
    # dynamicgexf.write_gexf(self.graph,outfile,version='1.2draft')
    nx.readwrite.gexf.write_gexf(self.graph,outfile,encoding='utf-8',version='1.2draft')

samples = ['hybrid-organizations', 'fluid-organizations']

full_coauthor_network = AuthorNetwork(directed=False)
full_citation_network = AuthorNetwork()

for sample in samples:
  coauthor_network = AuthorNetwork(directed=False)
  citation_network = AuthorNetwork()
  with open('data/02-refined/%s-articledata.json' % sample, 'rU') as f:
    articleset = json.load(f)

  for article in articleset:
    authors = article['authors']
    for index_a in range(0,len(authors)):
      for index_b in range(index_a+1,len(authors)):
        print('%s-%s' % (authors[index_a].strip(),authors[index_b].strip()))
        coauthor_network.link_authors(authors[index_a].strip(),authors[index_b].strip(),'coauthorship',sample)
        full_coauthor_network.link_authors(authors[index_a].strip(),authors[index_b].strip(),'coauthorship',sample)

    for reference in article['referencelist']:
      reference_authors = reference['authors']
      for reference_author in reference_authors:
        for author in authors:
          citation_network.link_authors(author.strip(),reference_author,'reference',sample)
          full_citation_network.link_authors(author.strip(),reference_author,'reference',sample)

  with open('data/04-network/%s-citation-network.gexf' % sample, 'wb') as f:
    citation_network.serialize(f)
  with open('data/04-network/%s-coauthor-network.gexf' % sample, 'wb') as f:
    coauthor_network.serialize(f)

with open('data/04-network/full-citation-network.gexf', 'wb') as f:
  full_citation_network.serialize(f)
with open('data/04-network/full-coauthor-network.gexf', 'wb') as f:
  full_coauthor_network.serialize(f)

# -*- coding: utf-8 -*-

'''

'''
import time
import argparse
import pandas as pd
import csv
import re
import json
from networkx import nx
# import dynamicgexf
from networkx.algorithms import centrality

# from titlecase import titlecase

import os,time
from pprint import pprint

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
  csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
  for row in csv_reader:
    yield [cell for cell in row]
    # yield [str(cell, 'utf-8') for cell in row]

def parse_legend(row):
  field_index = {}
  for index,column in enumerate(row):
    if column.startswith(u'\ufeff'):
      column = re.split(u'\ufeff', column)[1]

    field_index[column] = index
  return field_index

class ScopusParser:

  def __init__(self, author_fix_list_file=None):
    self.articlelist = []
    self.errors = []
    self.rows = []
    self.skipped = []
    self.swapped = []
    self.author_fix_list = dict()
    self.parse_author_fix_list(author_fix_list_file)

  def author_name(self, author):
    if len(author.strip()) > 35:
      return False
    return author

  def harmonize_author_name(self,author):
    print(author)
    # Takes care of names such as Lin B.-W.
    name = ('.').join(author.split('.-'))
    # >>> name = 'VON HIPPEL, E.'
    # >>> titlecase(name)
    # 'Von Hippel, E.'
    # TODO: re-install titlecase - if needed!?
    # name = titlecase(name)
    if name in self.author_fix_list:
      name = self.author_fix_list[name]
    # print 'Is name != author: %s != %s' % (name,author)
    if name != author:
      self.swapped.append([author,name])
    return name

  def parse_author_fix_list(self, author_fix_list_file=None):
    # print csv.list_dialects()
    if author_fix_list_file != None:
      with open(author_fix_list_file, 'rU') as f:
        reader = unicode_csv_reader(f)
        header = reader.next()
        if not header[0] == 'Author':
          raise Exception('Skipping header failed')
        for row in reader:
          self.author_fix_list[row[0]] = row[1]

  def parse(self, reader, rows_to_include):
    cols = parse_legend(reader.__next__())
    pprint(cols)
    # articlelist = []
    rownumber = 0
    for row in reader:
      print('\nRow #%s' % rownumber)
      if rownumber < rows_to_include:
        rowdata = {
          'number': rownumber,
          'raw': row,
          'article': None,
          'referencelist': None,
          'errors': None
        }
        article = {
          'authors': [],
          'original': None,
          'cols': {},
          'referencelist': [],
        }
        for col in cols:
          # print col
          # MongoDB does not allow periods in key names
          article['cols'][col.replace('.','')] = row[cols[col]]
        # print row[col['Authors']]
        print(row[cols[u'Authors']])
        raw_authors = row[cols[u'Authors']].split('.,')
        print(raw_authors)
        authors = list()
        for index, author in enumerate(raw_authors):
          if index < len(raw_authors)-1:
            print(index, author)
            author = '%s.' % author
          authors.append(author.replace(',',''))
        print(authors)
        # time.sleep(10)
        if row[0] == '[No author name available]':
          authors = []
        for author in authors:
          article['authors'].append(self.harmonize_author_name(self.author_name(author.strip())))
        article['title'] = row[cols['Title']]
        print(row[cols['Title']])
        print(article['authors'])
        # for index_a in range(0,len(authors)):
        #   for index_b in range(index_a+1,len(authors)):
        #     print '%s > %s' % (authors[index_a].strip(),authors[index_b].strip())
        #     # coauthor_network.link_authors(authors[index_a].strip(),authors[index_b].strip(),'coauthorship')

        print('\nReferences')

        references = row[cols['References']].split(';')
        # print json.dumps(references,indent=2)

        errors = []
        for reference_string in references:
          print(reference_string)
          try:
            reference = {}
            # Check for reference type and route?
            # TODO
            # Parsing the authors and other data from a reference
            reference_authors = []
            # Author name has to include a comma, e.g. Huhtamäki, J. or Russell, M.G.
            # A very rudimentary check to exclude e.g. 'The determinants of trust in supplier-automaker relationships in the U.S.'
            # from the author list
            author_pattern = re.compile('.+, (\.| |\-)*.{1}\.')

            name_part = None
            for reference_author in re.split('\.,', reference_string):
              # A brute-force fix for people with Jr. in their names, e.g. Churchill Jr G.A.
              if reference_author.endswith(' Jr') or reference_author.endswith(' Sr'):
                name_part = reference_author + '.,'
                continue
              if name_part != None:
                reference_author = name_part + reference_author
                name_part = None
                print('Reference author appended: %s' % reference_author)
                print(reference_string.strip())
                # time.sleep(4)
              reference_author = reference_author.strip() + '.'
              # print 'Refence authorreference_author
              # time.sleep(1)
              if not author_pattern.match(reference_author):
                print('Skipping author: %s' % reference_author)
                self.skipped.append({'author': reference_author, 'reference': reference_string})
                # time.sleep(4)

              if author_pattern.match(reference_author) and not re.search('[0-9\(\)]+', reference_author):
                print(reference_author)
                reference_author_refined = ''
                for piece in re.split(',', reference_author):
                  reference_author_refined += piece

                reference_author_refined = self.author_name(reference_author_refined)

                if reference_author_refined:
                  try:
                    reference_authors.append(self.harmonize_author_name(reference_author_refined))
                  except:
                    print(reference_string.strip())
                    raise
                else:
                  self.skipped.append({'author': reference_author, 'reference': reference_string})
                  print('Skipping author: %s' % reference_author)

            # print 'Authors is references:'
            # Seeking for the article year
            print(reference_string)
            year_pat = re.compile(r'(\(\d{4}\))')
            m=re.findall(year_pat,reference_string)
            pprint(m)
            year = m[0][1:5]
            pprint(year)
            # Putting the details together
            # print reference_authors
            reference['authors'] = reference_authors
            reference['year'] = year
            reference['original'] = reference_string.strip()
            pprint(reference_authors)
                  # for author in authors:
                #   # citation_network.link_authors(author.strip(),reference_author_refined,'reference')
                #   print '%s>%s' % (author.strip(),reference_author)
            article['referencelist'].append(reference)
            # time.sleep(2)
            # os.system('cls' if os.name=='nt' else 'clear')
            # print '\n'
          except IndexError as ie:
            print('Skipping reference: year missing?')
            errors.append(reference_string)

        print('Appending article')
        self.articlelist.append(article)
        rowdata['article'] = article
        rowdata['referencelist'] = article['referencelist']
        rowdata['errors'] = errors
        self.rows.append(rowdata)
      else:
        print('Skipping row #%s' % rownumber)
      rownumber += 1

  def serialize(self,nameseed):
    with open('data/02-refined/%s-articledata.json' % nameseed, 'w') as f:
      json.dump(self.articlelist,f,indent=2,sort_keys=True)
    with open('data/02-refined/%s-errors.json' % nameseed, 'w') as f:
      json.dump(self.errors,f,indent=2,sort_keys=True)
    with open('data/02-refined/%s-rows.json' % nameseed, 'w') as f:
      json.dump(self.rows,f,indent=2,sort_keys=True)
    with open('data/02-refined/%s-skipped.json' % nameseed, 'w') as f:
      json.dump(self.skipped,f,indent=2,sort_keys=True)
    with open('data/02-refined/%s-swapped.json' % nameseed, 'w') as f:
      json.dump(self.swapped,f,indent=2,sort_keys=True)


if __name__ == '__main__':
  argparser = argparse.ArgumentParser(description='Reference list reader')
  # argparser.add_argument('articlelist', type=argparse.FileType('rU'),
  #   help='file containing the company list data')
  # argparser.add_argument('coauthorfile', type=argparse.FileType('w'),
  #                       help='file for co-author network')
  # argparser.add_argument('refined', type=argparse.FileType('w'),
  #                       help='file for refined/cleaned data')
  samples = [
    {
      'file': 'data/01-source/hybrid-organizations.csv',
      'nameseed': 'hybrid-organizations',
      'rowstoinclude': 10000
    },
    {
      'file': 'data/01-source/fluid-organizations.csv',
      'nameseed': 'fluid-organizations',
      'rowstoinclude': 10000
    },
  ]
  author_fix_list = 'data/01-curate/authors-fixlist-empty.csv'

  for sample in samples:
    # TODO Switch to use Pandas instead of this hack.
    print('Processing sample %s' % sample['nameseed'])
    df = pd.read_csv(sample['file'])
    print(df.head())
    # Let's hope that charsets issues are less likely to occur in Python3
    articlelist = open(sample['file'], 'rU')
    args = argparser.parse_args()
    reader = unicode_csv_reader(articlelist)
    print(reader)
    # parser = ScopusParser(author_fix_list)
    parser = ScopusParser()
    parser.parse(reader, sample['rowstoinclude'])
    parser.serialize(sample['nameseed'])
    articlelist.close()

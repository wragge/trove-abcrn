from pymongo import MongoClient
from operator import itemgetter
from datetime import datetime
import calendar
import pickle
import nltk
import os
import re
from pymongo import ASCENDING, DESCENDING

def get_programs():
	client = MongoClient()
	db = client.trove_abcrn
	programs = db.programs
	programs.create_index([('date', ASCENDING)])
	return programs

def make_list(value):
	if not isinstance(value, list):
		value = [value]
	return value

def get_keyword_frequencies():
	scores = {}
	programs = get_programs()
	for program in programs.find():
		try:
			subjects = make_list(program['record'][0]['subject'])
			for subject in subjects:
				try:
					kw = subject.encode('utf-8').strip()
				except AttributeError:
					kw = str(subject).strip()
				try:
					scores[kw] += 1
				except KeyError:
					scores[kw] = 1
		except KeyError:
			pass
	scores = sorted(scores.items(), key=itemgetter(1), reverse=True)
	with open('keywords.csv', 'wb') as keywords:
		for f in scores:
			print '{}: {}'.format(f[0], f[1])
			keywords.write('{}, {}\n'.format(f[0], f[1]))

def write_abstracts_to_file():
	with open('abstracts.txt', 'wb') as abstracts:
		programs = get_programs()
		for program in programs.find().sort('date'):
			try:
				abstract = program['record'][0]['abstract'][0]
				abstracts.write('{}\n'.format(abstract.encode('utf-8')))
			except KeyError:
				pass


def write_programs_by_month():
	programs = get_programs()
	for year in range(2003, 2014):
		for month in range(1,13):
			start = datetime(year, month, 1)
			if month < 12:
				end = datetime(year, month + 1, 1)
			else:
				end = datetime(year + 1, 1, 1)
			stories = programs.find({'date': {'$gte': start, '$lt': end}, 'record.isPartOf': {'$in': ['ABC Radio. The World Today', 'ABC Radio. The World Today Archive Archive']}})
			filename = 'data/today/{}-{}.txt'.format(year, month)
			with open(filename, 'wb') as outfile:
				for story in stories:
					try:
						print '{}: {}'.format(story['date'], story['record'][0]['title'].encode('utf-8'))
					except (KeyError, AttributeError):
						#print story
						pass
					else:
						outfile.write('{}\n{}\n'.format(story['record'][0]['title'].encode('utf-8'), story['record'][0]['abstract'][0].encode('utf-8')))

def add_datetime():
	''' Add this to the harvesting function '''
	programs = get_programs()
	for program in programs.find():
		try:
			date_str = make_list(program['record'][0]['date'])
		except KeyError:
			#print program
			pass
		else:
			if '/' in date_str[0]:
				parts = date_str[0].split('/')
			else:
				parts = date_str[0].split('-')
			try:
				date_obj = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
			except ValueError:
				try:
					date_obj = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
				except ValueError:
					date_obj = None
			program['date'] = date_obj
			programs.save(program)

def calculate_tfidf_byyear():
    path = '/Users/tim/mycode/trovedata/trovedata/abcrn/data/today/'
    stopwords = nltk.corpus.stopwords.words('english')
    corpus = nltk.corpus.PlaintextCorpusReader(path, '.*\.txt')
    texts = nltk.text.TextCollection(corpus)
    for fileid in corpus.fileids():
    	print fileid
    	values = {}
        text = corpus.words(fileid)
        words = sorted(set(text))
        words = [word for word in words if word.isalpha() and word.lower() not in stopwords]
        for word in words:
            tfidf = texts.tf_idf(word, text)
            values[word] = tfidf
            #print '{}: {}'.format(word, tfidf)
        values = sorted(values.items(), key=itemgetter(1), reverse=True)
        with open('today-tfidf.txt', 'a') as outfile:
        	outfile.write('{},{}\n'.format(fileid.replace('.txt', ''), values[0][0]))
        print values[0]


def get_words(program):
	with open('{}-tfidf.txt'.format(program), 'rb') as infile:
		values = {}
		for value in infile:
			date, word = value.split(',')
			values[date.replace('-', '')] = word.strip()
		return values


def make_data_file():
	words = {}
	programs = ['pm', 'am', 'today']
	for program in programs:
		words[program] = get_words(program)
	data = []
	for year in range(2003, 2014):
		months = {'year': year, 'rows': []}
		for month in range(1,13):
			row = {}
			row['month'] = str(month).zfill(2)
			row['title'] = '{} {}'.format(calendar.month_name[month], year)
			for program in programs:
				try:
					row[program] = words[program]['{}{}'.format(year, month)]
				except KeyError:
					row[program] = ''
			months['rows'].append(row.copy())
		data.append(months.copy())
	print data
	with open('words_data.pickle', 'wb') as data_file:
		pickle.dump(data, data_file)


def get_reporter_names():
	names = {}
	programs = get_programs()
	for program in programs.find({'record.isPartOf': {'$in': ['ABC Radio. AM', 'ABC Radio. AM Archive', 'ABC Radio. PM', 'ABC Radio. PM Archive', 'ABC Radio. The World Today', 'ABC Radio. The World Today Archive']}}):
		contributors = make_list(program['record'][0]['contributor'])
		for contributor in contributors:
			contributor = re.sub(r'\(.*\)', '', contributor).strip()
			#print contributor
			try:
				names[contributor.title()] += 1
			except KeyError:
				names[contributor.title()] = 1
	#names = set(names)
	names = sorted(names.items(), key=itemgetter(1), reverse=True)
	for n in names:
		print '{}: {}'.format(n[0], n[1])


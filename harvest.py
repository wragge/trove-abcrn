from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from trove_python.trove_core import trove
from trove_python.trove_harvest.harvest import TroveHarvester
import credentials


class ABCRNHarvester(TroveHarvester):

	def set_collection(self):
		client = MongoClient()
		db = client.trove_abcrn
		programs = db.programs
		programs.remove()
		self.collection = programs

	def process_results(self, results):
		works = results[0]['records']['work']
		for work in works:
			versions = work['version']
			for version in versions:
				version['work_id'] = work['id']
				try:
					if version['record'][0]['metadataSource']['value'] == 'ABC:RN':
						# do all the things
						version['_id'] = version['id'].split()[0]
						try:
							version_id = self.collection.insert(version)
							print version_id
						except DuplicateKeyError:
							pass
						#print 'Added {}'.format(version_id)
				except (KeyError, TypeError):
					pass
		self.harvested += self.get_highest_n(results)
		print 'Harvested: {}'.format(self.harvested)

def do_harvest():
	query = 'http://api.trove.nla.gov.au/result?q=nuc:"ABC:RN"&zone=music&encoding=json&reclevel=full&include=workVersions'.format(credentials.TROVE_API_KEY)
	trove_api = trove.Trove(credentials.TROVE_API_KEY)
	harvester = ABCRNHarvester(trove_api, query=query)
	harvester.set_collection()
	harvester.harvest()


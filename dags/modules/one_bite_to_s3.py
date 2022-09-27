from requests import session 
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
from multiprocessing.pool import ThreadPool
import json
from modules.aws import awsHandler
from datetime import timedelta, datetime
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class oneBiteToS3Operator(BaseOperator):
	""" An operator used to download data from One Bite API and load that data to S3 """
	
	@apply_defaults
	def __init__(
		
			self,
			bucket: str,
			*args, 
			**kwargs
		)-> None:

		super().__init__(*args, **kwargs)
		self.bucket = bucket
		self.url = 'https://api.onebite.app/review'
		self.offset = 0
		self.session = session()
		self.aws_instance = awsHandler()
		self.date_json_dict = {}


	def get_run_config(self, context) -> None:
		""" Retrieves manual trigger config if exists and creates run date parameters 

		Airflow allows manual trigger config to be passed to a Dag run. If you want to backfill this pipeline, you'll need to 
		manually run the Dag and pass the date parameters to the run in the following format {"backfill_start": "", "backfill_end": ""}. 
		If this date dictionary is not passed at runtime, the run config will rely on the execution date provided by airflow templates. 
		This function allows creates a date_range object that is used downstream to filter results. 
		
		"""

		self.backfill_status = context['ti'].xcom_pull(dag_id = 'one_bite', task_ids='set_run_config' , key="backfill_status")
		self.run_start = context['ti'].xcom_pull(dag_id = 'one_bite', task_ids='set_run_config' , key="run_start")
		self.run_end = context['ti'].xcom_pull(dag_id = 'one_bite', task_ids='set_run_config' , key="run_end")        
		self.run_start_fmt = datetime.strptime(self.run_start, '%Y-%m-%d').date()
		self.run_end_fmt = datetime.strptime(self.run_end, '%Y-%m-%d').date()
		self.date_range = [datetime.strftime(self.run_start_fmt+timedelta(days=x),'%Y-%m-%d') for x in range((self.run_end_fmt-self.run_start_fmt).days + 1)]
		
		
	def scrape_reviews(self, offset:str):
		""" Requests API response for specific offset of onebite app review list 
		
		Args:
			offset: the page being requested. Page limits are 30 results per page. Offsets increment by 30.
		
		Returns:
			offset_json_list: a list of review objects for the current offset
			max_date_list: the max date of the objects in the offset_json_list

		"""

		params = {'offset': offset}
		
		r = self.session.get(url=self.url, params = params)
		
		reviews_json = r.json()
			
		offset_json_list = []
		date_list = []

		for record in reviews_json:
			partition_date = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
			record['partition_date'] = partition_date
			date_list.append(partition_date)
			offset_json_list.append(record)

		max_date_list = max(date_list)

		return offset_json_list, max_date_list


	def execute(self, context) -> None:
		""" The code to execute when the runner calls the operator """

		self.get_run_config(context)

		for date in self.date_range:
			self.date_json_dict[date] = []

		with concurrent.futures.ThreadPoolExecutor() as executor:
			more = True
			while more:
				future = executor.submit(self.scrape_reviews, self.offset)
				offset_json_list, max_date = future.result()
				print(self.offset, max_date)
				for review in offset_json_list:
					if review['partition_date'] < self.run_start_fmt:
						print("Break Loop.")
						more = False
						break
					elif review['partition_date'] > self.run_end_fmt:
						pass
					else: 
						partition_date_str = review['partition_date'].strftime('%Y-%m-%d') 
						review['partition_date'] = partition_date_str
						self.date_json_dict[partition_date_str].append(review)
				self.offset +=30
			
			for k, v in self.date_json_dict.items():
				self.aws_instance.upload_json_to_s3(
					obj = json.dumps(v), 
					bucket = self.bucket, 
					file_name = f'partition_date={k}/{k}.json'
					)

		print("Done.")
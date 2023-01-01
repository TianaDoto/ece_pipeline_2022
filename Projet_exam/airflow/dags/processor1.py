from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from nifi_processor import getFile, routeOnAttribute, putFile1, \
	updateAttributeTour1, updateAttributeTour2, \
	publishKafkaRecord_2_6Tour1, publishKafkaRecord_2_6Tour2, \
	putFileTour1, putFileTour2


with DAG ("DAG_ECE_pipeline",
	schedule = timedelta(days=1),
	start_date =  datetime(2022, 1, 1),
	catchup =False
	) as dag :
	  
	download_file = BashOperator(
		task_id='download_file',
		bash_command='python3 /home/heritiana/Project/Projet_exam/scripts/download_file.py'
	)

	get_file = PythonOperator(
		task_id = "get_file",
		python_callable = getFile
	)
	
	route_on_attribute = PythonOperator(
		task_id = "route_on_attribute",
		python_callable = routeOnAttribute
	)
	
	put_file1 = PythonOperator(
		task_id = "put_file1",
		python_callable = putFile1
	)

	with TaskGroup(group_id="put_and_publish_out_tour1") as put_and_publish_out_tour1:
		with TaskGroup(group_id="update_tour1") as update_tour1:
			update_Attribute_Tour1 = PythonOperator(
				task_id = "update_Attribute_Tour1",
				python_callable = updateAttributeTour1
			)
		with TaskGroup(group_id="put_and_publish_tour1") as put_and_publish_tour1:
			put_file_tour1 = PythonOperator(
				task_id = "put_file_tour1",
				python_callable = putFileTour1
			)
			
			publish_Kafka_Record_2_6Tour1 = PythonOperator(
				task_id = "publish_Kafka_Record_2_6Tour1", 
				python_callable = publishKafkaRecord_2_6Tour1
			)

			spark_run_tour1 = BashOperator(
				task_id = "spark_run_tour1", 
				bash_command='spark-submit /home/heritiana/Project/Projet_exam/scripts/transformation_tour1.py'
			)

			put_file_tour1 >> spark_run_tour1
			
		update_tour1 >> put_and_publish_tour1

	with TaskGroup(group_id="put_and_publish_out_tour2") as put_and_publish_out_tour2:
		with TaskGroup(group_id="update_tour2") as update_tour2:
			update_Attribute_Tour2 = PythonOperator(
				task_id = "update_Attribute_Tour2",
				python_callable = updateAttributeTour2
			)
		with TaskGroup(group_id="put_and_publish_tour2") as put_and_publish_tour2:
			put_file_tour2 = PythonOperator(
				task_id = "put_file_tour2",
				python_callable = putFileTour2
			)

			publish_Kafka_Record_2_6Tour2 = PythonOperator(
				task_id = "publish_Kafka_Record_2_6Tour2",
				python_callable = publishKafkaRecord_2_6Tour2
				)

			spark_run_tour2 = BashOperator(
				task_id = "spark_run_tour2", 
				bash_command='spark-submit /home/heritiana/Project/Projet_exam/scripts/transformation_tour2.py'
			)

			put_file_tour2 >> spark_run_tour2
		update_tour2 >> put_and_publish_tour2

	download_file >> get_file >> route_on_attribute >> [put_file1, put_and_publish_out_tour1, put_and_publish_out_tour2]

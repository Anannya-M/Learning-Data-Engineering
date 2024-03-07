import os

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'


env = os.environ['env']
header = os.environ['header']
inferSchema = os.environ['inferSchema']


#Defining appName
appName = 'PySpark Project'

#Obtaining the current Working Directory
current = os.getcwd()

#Defining the path to the data
source_olap = current + 'source//olap'
source_oltp = current + 'source//oltp'
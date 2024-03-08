import os

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'root'
os.environ['password'] = 'root'

env = os.environ['env']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

user = os.environ['user']
password = os.environ['password']

# Defining appName
appName = 'PySpark Project'

# Obtaining the current Working Directory
current = os.getcwd()

# Defining the path to the data
source_olap = current + '\\source\\olap'
source_oltp = current + '\\source\\oltp'

# Defining the path to the output
city_path = 'output\\cities'
presc_path = 'output\\prescriber'

from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import json
import ssl


# code to initialize and connect the elasticsearch server
context = ssl.create_default_context(cafile="D:\elasticsearch-8.15.3\config\certs\http_ca.crt")
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=("elastic", "PRZ86TrfKjH4kee7LwwV"),
    ssl_context=context
)


# Create Collection
def createCollection(p_collection_name):

    p_collection_name = p_collection_name.lower()

    # checking if indices is exists or not
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection {p_collection_name} created.")
    else:
        print(f"Collection {p_collection_name} already exists.")


# Index Data
def indexData(p_collection_name, data_type):

    p_collection_name = p_collection_name.lower()

    # reading data from the csv file
    data = pd.read_csv('Employee.csv', encoding='ISO-8859-1')
    data = data.replace({np.nan: None})
    
    # Iterating each rows from the data and indexing each data
    for _, row in data.iterrows():
        try:
            es.index(index=p_collection_name, document=row.to_dict())
        except Exception as e:
            print(f"Failed to index document: {e}")
    
    print(f"Data has been indexed successfully in the '{p_collection_name}' collection.")


# Search by Column
def searchByColumn(p_collection_name, p_column_name, p_column_value):

    p_collection_name = p_collection_name.lower()

    # qurey to get the matching column
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }

    # query the column in the indices
    result = es.search(index=p_collection_name, body=query)
    print("Search Results:")
    print(json.dumps(result['hits']['hits'], indent=4)) 


# Get Employee Count
def getEmpCount(p_collection_name):

    p_collection_name = p_collection_name.lower()
    
    # Fetching the count in the index
    try:
        count = es.count(index=p_collection_name)['count']
        print(f"Document count in '{p_collection_name}': {count}")
    except Exception as e:
        print(f"Index '{p_collection_name}' not found. Error: {e}")
        

# Delete Employee by ID
def delEmpById(p_collection_name, p_employee_id):

    p_collection_name = p_collection_name.lower()

    # query to get the Employee ID
    search_body = {
        "query": {
            "term": {
                "Employee ID.keyword": p_employee_id
            }
        }
    }
    
    # querying the Employee details from the index using Employee ID
    search_response = es.search(index=p_collection_name, body=search_body)
    if search_response['hits']['total']['value'] > 0:
        doc_id = search_response['hits']['hits'][0]['_id']

        # Deleting the Employee data based on the Employee ID
        try:
            es.delete(index=p_collection_name, id=doc_id)
            print(f"Document with Employee ID {p_employee_id} deleted successfully.")
        except Exception as e:
            print(f"Failed to delete document with Employee ID {p_employee_id}. Error: {e}")
    else:
        print(f"Document with Employee ID {p_employee_id} does not exist.")


# Get Department Facet
def getDepFacet(p_collection_name):

    p_collection_name = p_collection_name.lower()

    # Query to get the department facet
    query = {
        "size": 0,
        "aggs": {
            "department_counts": {
                "terms": {"field": "Department.keyword"}
            }
        }
    }

    result = es.search(index=p_collection_name, body=query)
    dep_counts = result['aggregations']['department_counts']['buckets']
    print("Department Facet Results:")
    print(json.dumps(dep_counts, indent=4))


# a) Collection Name
v_nameCollection = "Lokeshwaran"

# b) Collection Name
v_phoneCollection = "6419" 

# c) createCollection
createCollection(v_nameCollection)

# d) createCollection(v_phoneCollection)
createCollection(v_phoneCollection)

# e) Get Employee Count
getEmpCount(v_nameCollection)

# f) Index Data in v_nameCollection excluding 'Department'
indexData(v_nameCollection, 'Department')

# g) Index Data in v_phoneCollection excluding 'Gender'
indexData(v_phoneCollection, 'Gender')

# h) Get Employee Count after Indexing
getEmpCount(v_nameCollection)

# i) Delete Employee by ID
delEmpById(v_nameCollection, 'E02003')

# j) Get Employee Count after Deletion
getEmpCount(v_nameCollection)

# k) Search by Column
print(searchByColumn(v_nameCollection, 'Department', 'IT'))

# l) Search by Column
print(searchByColumn(v_nameCollection, 'Gender', 'Male'))

# m) Search in v_phoneCollection
print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

# n) Get Department Facet for v_nameCollection
print(getDepFacet(v_nameCollection))

# o) Get Department Facet for v_phoneCollection
print(getDepFacet(v_phoneCollection))

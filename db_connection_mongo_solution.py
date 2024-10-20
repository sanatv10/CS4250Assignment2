#-------------------------------------------------------------------------
# AUTHOR: Sanat Vankayalapati
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: This Python program connects to a MongoDB database using PyMongo to perform CRUD operations on documents and generate an inverted index of terms from the document collection.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 days
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import re

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    client = MongoClient('localhost', 27017)  
    db = client['document_db']  
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
  

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    

    #Producing a final document as a dictionary including all the required fields
    # --> add your Python code here

    # Insert the document
    # --> add your Python code here
    
    words = re.findall(r'\b\w+\b', docText.lower())  
    term_frequency = {}
    for word in words:
        term_frequency[word] = term_frequency.get(word, 0) + 1

    
    term_list = [{"term": word, "count": count, "num_chars": len(word)} for word, count in term_frequency.items()]

    
    
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_list
    }

    
    col.insert_one(document)
    

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})
    


def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here

    # Create the document with the same id
    # --> add your Python code here

    deleteDocument(col, docId)

    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here

    inverted_index = {}
    
    for document in col.find():
        title = document.get("title", "")
        for term_info in document.get("terms", []):
            term = term_info["term"].lower()  
            count = term_info["count"]
            
            if term not in inverted_index:
                inverted_index[term] = f"{title}:{count}"
            else:
                inverted_index[term] += f", {title}:{count}"
                
    return inverted_index

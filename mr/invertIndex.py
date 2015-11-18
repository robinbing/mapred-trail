__author__ = 'admin'
import os
import MapReduce

os.chdir("D:/github/mapred-trail/data")
mr = MapReduce.MapReduce()

def mapper(recods):
    # key = documents identifier
    # valuse = text
    key = recods[0]
    value = recods[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)

def reduce(key, document_id):
    total = []
    for id in document_id:
        total.append(id)
    mr.emit((key, total))

books = open('books.json')
mr.execute(books, mapper, reduce)
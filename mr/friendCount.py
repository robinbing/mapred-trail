__author__ = 'admin'
import os
import MapReduce

os.chdir("D:/github/mapred-trail/data")
mr = MapReduce.MapReduce()

def mapper(rela):
    key = rela[0]
    mr.emit_intermediate(key, 1)

def reduce(key, list_of_value):
    total = 0
    for value in list_of_value:
        total += value
    mr.emit((key, total))

books = open('friends.json')
mr.execute(books, mapper, reduce)
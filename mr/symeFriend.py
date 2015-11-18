__author__ = 'admin'
import os
import MapReduce

os.chdir("D:/github/mapred-trail/data")
mr = MapReduce.MapReduce()

def mapper(rela):
    A = rela[0]
    B = rela[1]
    mr.emit_intermediate((A + ' ' + B), 1)
    mr.emit_intermediate((B + ' ' + A), 1)

def reduce(key, list_of_value):
    total = 0
    for value in list_of_value:
        total += value
    if value == 1:
        mr.emit(key.split(' '))

books = open('friends.json')
mr.execute(books, mapper, reduce)

# 2nd way

def mapper(rela):
    A = rela[0]
    B = rela[1]
    mr.emit_intermediate(A, B)
    mr.emit_intermediate(B, A)

def reduce(key, list_of_value):
    d = {}
    for value in list_of_value:
        if d.get(value) is None:
            d[value] = 1
            mr.emit((key,value))

books = open('friends.json')
mr.execute(books, mapper, reduce)

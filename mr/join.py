__author__ = 'admin'
import os
import MapReduce

os.chdir("D:/github/mapred-trail/data")
mr = MapReduce.MapReduce()

def mapper(data):
    # key: identify the table the record originates from
    # value: order_id
    key = data[1]
    value = data
    mr.emit_intermediate(key, value)

def reduce(key, list_of_value):
    for record in list_of_value:
        if record[0] == 'order':
            for record_2 in list_of_value:
                if record_2[0] == 'line_item':
                    mr.emit((record + record_2))

books = open('records.json')
mr.execute(books, mapper, reduce)
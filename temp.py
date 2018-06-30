from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Data_process.data_extraction import file_name
import re
import time


graph = Con_Neo4j(http='http://127.0.0.1:7474', username='neo4j', password='123456')


if __name__ == '__main__':
    with open('Data/AStack_com_invest.csv') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for row in rows:
            print(row)
from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Data_process.data_extraction import file_name
import re
import time


graph = Con_Neo4j(http='http://127.0.0.1:7474', username='neo4j', password='123456')


if __name__ == '__main__':
    with open('Data/company.csv', mode='r', encoding='utf-8', newline='') as csvfile, open('Data/company1.csv', mode='w', encoding='utf-8', newline='') as csvfile2:
        rows = csv.reader(csvfile, delimiter=';')
        writer = csv.writer(csvfile2, delimiter='\t')
        for row in rows:
            writer.writerow(row)
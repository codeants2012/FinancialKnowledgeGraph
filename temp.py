from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Data_process.data_extraction import file_name
import re
import time


# graph = Con_Neo4j(http='http://127.0.0.1:7474', username='neo4j', password='123456')


if __name__ == '__main__':
    files = ['AStack_com_block', 'AStack_com_industry', 'AStack_com_invest', 'AStack_com_output']
    with open('Data/Encoding/com_name_edge.csv', mode='w', encoding='utf-8', newline='') as file1:
        writer = csv.writer(file1, delimiter='\t')
        for csv_name in files:
            csv_path = 'Data/' + csv_name + '.csv'
            with open(csv_path, mode='r', encoding='utf-8', newline='') as file2:
                rows = csv.reader(file2, delimiter='\t')
                if csv_name == 'AStack_com_block':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], 'BLOCK_'+row[3]]
                        writer.writerow(writer_row)

                if csv_name == 'AStack_com_industry':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], 'INDUSTRY_'+row[4]]
                        writer.writerow(writer_row)

                if csv_name == 'AStack_com_invest':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], row[6]]
                        writer.writerow(writer_row)
                if csv_name == 'AStack_com_output':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], row[6]]
                        writer.writerow(writer_row)
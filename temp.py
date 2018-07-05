from py2neo import Graph, Node, Relationship
import csv
import re
import time


# 连接图数据库
graph = Graph('http://127.0.0.1:7474', username='neo4j', password='123456', bolt=True, secure=False)


if __name__ == '__main__':
    files = ['AStack_com_block', 'AStack_com_industry', 'AStack_com_invest', 'AStack_com_output']
    with open('Data/Export/com_name_edge_type.csv', mode='w', encoding='utf-8', newline='') as file1:
        writer = csv.writer(file1, delimiter='\t')
        for csv_name in files:
            csv_path = 'Data/' + csv_name + '.csv'
            with open(csv_path, mode='r', encoding='utf-8', newline='') as file2:
                rows = csv.reader(file2, delimiter='\t')
                if csv_name == 'AStack_com_block':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], 'BLOCK_' + row[3], 'COM_BelongTo_B']
                        writer.writerow(writer_row)

                if csv_name == 'AStack_com_industry':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], 'INDUSTRY_' + row[4], 'COM_BelongTo_IND']
                        writer.writerow(writer_row)

                if csv_name == 'AStack_com_invest':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], row[6], 'COM_Invest_COM']
                        writer.writerow(writer_row)
                if csv_name == 'AStack_com_output':
                    for i, row in enumerate(rows):
                        if i == 0:
                            continue
                        writer_row = [row[2], row[6], 'COM_Output_COM']
                        writer.writerow(writer_row)
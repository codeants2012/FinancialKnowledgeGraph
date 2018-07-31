from py2neo import Graph, Node, Relationship
import csv
from Data_process.data_extraction import file_name
import re
import time
import pandas as pd
from tqdm import tqdm

# 连接图数据库
graph = Graph('http://127.0.0.1:7474', username='neo4j', password='123456', bolt=True, secure=False)


def com_type(code):  # 识别公司代码所属类别，包括：深A、沪A、深B、沪B、三板、其它、非上市
    if len(code) >= 6:
        if code[0] in ['0', '3']:
            c_type = '深A'
        elif code[0] in ['6']:
            c_type = '沪A'
        elif code[0] in ['2']:
            c_type = '深B'
        elif code[0] in ['9']:
            c_type = '沪B'
        elif code[0] in ['4', '8']:
            c_type = '三板'
        else:
            c_type = '其它'
    elif code in ['', '-', '--']:
        c_type = '非上市'
    else:
        c_type = '其它'
    return c_type


def create_company():  # 在图中创建A股上市公司节点
    df1 = pd.read_csv('../Data/company.csv', encoding='utf-8', sep='\t').fillna('')
    keys = df1.columns.tolist()
    for i in tqdm(range(len(df1)), desc='create_company'):
        node = Node('COMPANY')
        for j, key in enumerate(keys):
            node[key] = df1.iloc[i, j]
        node['股票类型'] = com_type(df1.iloc[i, 0])
        graph.create(node)


def create_industry():  # 在图中的创建行业节点
    df1 = pd.read_csv('../Data/industry_tags_id.csv', encoding='utf-8', sep='\t').fillna('')
    keys = df1.columns.tolist()
    for i in tqdm(range(len(df1)), desc='create_industry'):
        node = Node('INDUSTRY')
        for j, key in enumerate(keys):
            node[key] = df1.iloc[i, j]
        graph.create(node)


def create_com_to_ind():  # 在图中创建公司与行业的关系
    df1 = pd.read_csv('../Data/AStack_com_industry.csv', encoding='utf-8', sep='\t').fillna('')
    for i in tqdm(range(len(df1)), desc='create_com_to_ind'):
        com_node = Node('COMPANY')
        com_node['股票代码'] = df1.iloc[i, 0]
        com_node['公司简称'] = df1.iloc[i, 1]
        com_node['公司全称'] = df1.iloc[i, 2]
        ind_name = Node('INDUSTRY')
        ind_name['分类体系'] = df1.iloc[i, 3]
        ind_name['行业名称'] = df1.iloc[i, 4]
        rel = Relationship(com_node, 'COM_BelongTo_IND', ind_name)
        graph.merge(rel)


def create_com_block():  # 在图中创建板块节点，以及A股上市公司与板块的关系
    df1 = pd.read_csv('../Data/AStack_com_block.csv', encoding='utf-8', sep='\t').fillna('')
    for i in tqdm(range(len(df1)), desc='create_com_block'):
        com_node = Node('COMPANY')
        com_node['股票代码'] = df1.iloc[i, 0]
        com_node['公司简称'] = df1.iloc[i, 1]
        com_node['公司全称'] = df1.iloc[i, 2]
        block_node = Node('BLOCK')
        block_node['板块名称'] = df1.iloc[i, 3]
        rel = Relationship(com_node, 'COM_BelongTo_B', block_node)
        graph.merge(rel)


def create_com_output():  # 在图中创建公司产业输出关系（上下游），如果公司节点不存在则创建
    df1 = pd.read_csv('../Data/AStack_com_output.csv', encoding='utf-8', sep='\t').fillna('')
    for i in tqdm(range(len(df1)), desc='create_com_output'):
        start_node = Node('COMPANY')
        start_node['股票代码'] = df1.iloc[i, 0]
        start_node['公司简称'] = df1.iloc[i, 1]
        start_node['公司全称'] = df1.iloc[i, 2]
        start_node['股票类型'] = df1.iloc[i, 3]
        end_node = Node('COMPANY')
        end_node['股票代码'] = df1.iloc[i, 4]
        end_node['公司简称'] = df1.iloc[i, 5]
        end_node['公司全称'] = df1.iloc[i, 6]
        end_node['股票类型'] = df1.iloc[i, 7]
        rel = Relationship(start_node, 'COM_Output_COM', end_node)
        rel['输出金额'] = df1.iloc[i, 8]
        rel['报告日期'] = df1.iloc[i, 9]
        graph.merge(rel)


def create_com_invest():  # 在图中创建公司投资关系，如果公司节点不存在则创建
    df1 = pd.read_csv('../Data/AStack_com_invest.csv', encoding='utf-8', sep='\t').fillna('')
    for i in tqdm(range(len(df1)), desc='create_com_invest'):
        start_node = Node('COMPANY')
        start_node['股票代码'] = df1.iloc[i, 0]
        start_node['公司简称'] = df1.iloc[i, 1]
        start_node['公司全称'] = df1.iloc[i, 2]
        start_node['股票类型'] = df1.iloc[i, 3]
        end_node = Node('COMPANY')
        end_node['股票代码'] = df1.iloc[i, 4]
        end_node['公司简称'] = df1.iloc[i, 5]
        end_node['公司全称'] = df1.iloc[i, 6]
        end_node['股票类型'] = df1.iloc[i, 7]
        rel = Relationship(start_node, 'COM_Invest_COM', end_node)
        rel['持股比例'] = df1.iloc[i, 8]
        rel['报告日期'] = df1.iloc[i, 9]
        graph.merge(rel)


def create_user_to_industry():  # 在图中创建用户节点，以及用户与行业的关系
    time1 = time.time()
    with open('../Data/User/user_1000_labels_2-8_ind.txt', mode='r', encoding='utf-8', newline='') as txtfile:
        rows = txtfile.readlines()
        for row in rows:
            pattern = re.compile(r'\d+')
            res = re.findall(pattern, row)
            # print(res)
            user_node = Node('USER')
            user_node['用户ID'] = res[0]
            graph.create(user_node)
            codes = res[1:]
            for code in codes:
                ind_node = Node('INDUSTRY')
                ind_node['行业代码'] = code
                rel = Relationship(user_node, 'U_FocusOn_IND', ind_node)
                graph.merge(ind_node | rel)
    time2 = time.time()
    print('Over: create_user_to_industry', time2 - time1)


def create_inf_to_labels():  # 在图中创建资讯节点，以及资讯与行业的关系
    time1 = time.time()
    file_path = '../Data/Information/inf_labels/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        label_name = (file.split('.'))[0]
        label_node = graph.find_one(label='INDUSTRY', property_key='行业名称', property_value=label_name)
        if not label_node:
            print(label_name)
            continue
        csvpath = file_path + file
        with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
            rows = csv.reader(csvfile, delimiter=';')
            for index, row in enumerate(rows):
                if index == 0:
                    continue
                inf_node = Node('INFORMATION')
                inf_node['资讯ID'] = row[0]
                inf_node['资讯标题'] = row[1]
                rel = Relationship(inf_node, 'INF_ReferTo_IND', label_node)
                graph.merge(inf_node | rel)
                rel_num += 1
                # print(rel_num, label_name, row)
    time2 = time.time()
    print('Over: create_inf_to_labels', time2 - time1)

import time
if __name__ == '__main__':
    create_company()
    create_industry()
    create_com_to_ind()
    create_com_block()
    create_com_output()
    # create_com_invest()
    # create_user_to_industry()
    # create_inf_to_labels()
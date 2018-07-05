from py2neo import Graph, Node, Relationship
import csv
from Data_process.data_extraction import file_name
import re
import time


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
    time1 = time.time()
    with open('../Data/company.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            node = Node('COMPANY')
            node['stock_code'] = row[0]
            node['chi_sht'] = row[1]
            node['com_name'] = row[2]
            node['eng_name'] = row[3]
            node['found_dt'] = row[4]
            node['reg_prov'] = row[5]
            node['old_name'] = row[6]
            node['legal_rep'] = row[7]
            node['ind_dir'] = row[8]
            node['acc_firm'] = row[9]
            node['sec_aff_rep'] = row[10]
            node['adv_ser'] = row[11]
            node['com_type'] = com_type(row[0])
            graph.create(node)
            # print(index, row)
    time2 = time.time()
    print('Over: create_company', time2 - time1)


def create_industry():  # 在图中的创建行业节点
    time1 = time.time()
    with open('../Data/industry_tags_id.txt', mode='r', encoding='utf-8', newline='') as txtfile:
        rows = txtfile.readlines()
        for row in rows:
            inds = row.replace('\n', '').split('\t')
            node = Node('INDUSTRY')
            node['ind_code'] = inds[0]
            node['ind_name'] = inds[1]
            node['class_system'] = '申万三级'
            graph.create(node)
            # print(inds)
    time2 = time.time()
    print('Over: create_industry', time2 - time1)


def create_com_to_ind():  # 在图中创建公司与行业的关系
    time1 = time.time()
    with open('../Data/AStack_com_industry.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            com_node = Node('COMPANY')
            com_node['stock_code'] = row[0]
            com_node['chi_sht'] = row[1]
            com_node['com_name'] = row[2]
            ind_name = Node('INDUSTRY')
            ind_name['class_system'] = row[3]
            ind_name['ind_name'] = row[4]
            rel = Relationship(com_node, 'COM_BelongTo_IND', ind_name)
            graph.merge(com_node | ind_name | rel)
    time2 = time.time()
    print('Over: create_com_to_ind', time2 - time1)


def create_com_block():  # 在图中创建板块节点，以及A股上市公司与板块的关系
    time1 = time.time()
    with open('../Data/AStack_com_block.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            com_node = Node('COMPANY')
            com_node['stock_code'] = row[0]
            com_node['chi_sht'] = row[1]
            com_node['com_name'] = row[2]
            block_node = Node('BLOCK')
            block_node['block_name'] = row[3]
            rel = Relationship(com_node, 'COM_BelongTo_B', block_node)
            graph.merge(com_node | block_node | rel)
    time2 = time.time()
    print('Over: create_com_block', time2 - time1)


def create_com_output():  # 在图中创建公司产业输出关系（上下游），如果公司节点不存在则创建
    time1 = time.time()
    with open('../Data/AStack_com_output.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            start_node = Node('COMPANY')
            start_node['stock_code'] = row[0]
            start_node['chi_sht'] = row[1]
            start_node['com_name'] = row[2]
            start_node['com_type'] = row[3]
            end_node = Node('COMPANY')
            end_node['stock_code'] = row[4]
            end_node['chi_sht'] = row[5]
            end_node['com_name'] = row[6]
            end_node['com_type'] = row[7]
            rel = Relationship(start_node, 'COM_Output_COM', end_node)
            rel['output_funt'] = row[8]
            rel['report_dt'] = row[9]
            graph.merge(start_node | end_node | rel)
    time2 = time.time()
    print('Over: create_com_output', time2 - time1)


def create_com_invest():  # 在图中创建公司投资关系，如果公司节点不存在则创建
    time1 = time.time()
    with open('../Data/AStack_com_invest.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            start_node = Node('COMPANY')
            start_node['stock_code'] = row[0]
            start_node['chi_sht'] = row[1]
            start_node['com_name'] = row[2]
            start_node['com_type'] = row[3]
            end_node = Node('COMPANY')
            end_node['stock_code'] = row[4]
            end_node['chi_sht'] = row[5]
            end_node['com_name'] = row[6]
            end_node['com_type'] = row[7]
            rel = Relationship(start_node, 'COM_Invest_COM', end_node)
            rel['proportion'] = row[8]
            rel['report_dt'] = row[9]
            graph.merge(start_node | end_node | rel)
    time2 = time.time()
    print('Over: create_com_invest', time2 - time1)


def create_user_to_industry():  # 在图中创建用户节点，以及用户与行业的关系
    time1 = time.time()
    with open('../Data/User/user_1000_labels_2-8_ind.txt', mode='r', encoding='utf-8', newline='') as txtfile:
        rows = txtfile.readlines()
        for row in rows:
            pattern = re.compile(r'\d+')
            res = re.findall(pattern, row)
            # print(res)
            user_node = Node('USER')
            user_node['user_id'] = res[0]
            graph.create(user_node)
            codes = res[1:]
            for code in codes:
                ind_node = Node('INDUSTRY')
                ind_node['ind_code'] = code
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
        label_node = graph.find_one(label='INDUSTRY', property_key='ind_name', property_value=label_name)
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
                inf_node['inf_id'] = row[0]
                inf_node['inf_title'] = row[1]
                rel = Relationship(inf_node, 'INF_ReferTo_IND', label_node)
                graph.merge(inf_node | rel)
                rel_num += 1
                # print(rel_num, label_name, row)
    time2 = time.time()
    print('Over: create_inf_to_labels', time2 - time1)


if __name__ == '__main__':
    create_company()
    create_industry()
    create_com_to_ind()
    create_com_block()
    create_com_output()
    create_com_invest()
    # create_user_to_industry()
    # create_inf_to_labels()
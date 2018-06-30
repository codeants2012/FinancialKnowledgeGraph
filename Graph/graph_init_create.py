from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Data_process.data_extraction import file_name
import re
import time


graph = Con_Neo4j(http='http://127.0.0.1:7474', username='neo4j', password='123456')


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
        rows = csv.reader(csvfile, delimiter=';')
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
    with open('../Data/com_industry_tags.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile)
        for index, row in enumerate(rows):
            if index == 0:
                continue
            com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=row[0])
            ind_node = graph.find_one(label='INDUSTRY', property_key='ind_name', property_value=row[2])
            if not com_node:
                continue
            if ind_node:
                com_rel = Relationship(com_node, 'COM_BelongTo_IND', ind_node)
                graph.create(com_rel)
            else:
                print('Missing industry:', index, row)
                new_node = Node('INDUSTRY')
                new_node['ind_name'] = row[2]
                new_node['class_system'] = '申万三级'
                com_rel = Relationship(com_node, 'COM_BelongTo_IND', new_node)
                graph.create(new_node | com_rel)
    time2 = time.time()
    print('Over: create_com_to_ind', time2 - time1)


def create_com_block():  # 在图中创建板块节点，以及A股上市公司与板块的关系
    time1 = time.time()
    file_path = '../Data/AStack_com_block/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
        if node:
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                for index, row in enumerate(rows):
                    if index == 0:
                        continue
                    rel_num += 1
                    # print(rel_num, stock_code, '-->', row)
                    block_node = graph.find_one(label='BLOCK', property_key='block_name', property_value=row[0])
                    if block_node:
                        rel = Relationship(node, 'COM_BelongTo_B', block_node)
                        graph.create(rel)
                    else:
                        nod = Node('BLOCK')
                        nod['block_name'] = row[0]
                        rel = Relationship(node, 'COM_BelongTo_B', nod)
                        graph.create(nod | rel)
    time2 = time.time()
    print('Over: create_com_block', time2 - time1)


def create_com_output():  # 在图中创建公司产业输出关系（上下游），如果公司节点不存在则创建
    time1 = time.time()
    file_path = '../Data/AStack_com_output/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        # if stock_code[0] not in ['0', '3', '6']:
        #     continue
        node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
        if node:
            if '上游' in file:  # 上游公司
                csvpath = file_path + file
                with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                    rows = csv.reader(csvfile, delimiter=';')
                    for index, row in enumerate(rows):
                        if index == 0:
                            continue
                        rel_num += 1
                        # print(rel_num, row, '-->', stock_code)
                        if row[3] not in ['', '-', '--']:
                            row[3] = float(row[3].replace(',', ''))
                        if row[1] != '-':
                            up_code = row[1]
                            node_up = graph.find_one(label='COMPANY', property_key='stock_code', property_value=up_code)
                            if node_up:
                                rel = Relationship(node_up, 'COM_Output_COM', node)
                                rel['report_dt'] = row[2]
                                rel['output_funt'] = row[3]
                                graph.create(rel)
                            else:
                                nod = Node('COMPANY')
                                nod['com_name'] = row[0]
                                nod['stock_code'] = row[1]
                                nod['com_type'] = com_type(row[1])
                                rel = Relationship(nod, 'COM_Output_COM', node)
                                rel['report_dt'] = row[2]
                                rel['output_funt'] = row[3]
                                graph.create(nod | rel)
                        else:
                            node_up = graph.find_one(label='COMPANY', property_key='com_name', property_value=row[0])
                            if node_up:
                                rel = Relationship(node_up, 'COM_Output_COM', node)
                            else:
                                nod = Node('COMPANY')
                                nod['com_name'] = row[0]
                                nod['com_type'] = com_type(row[1])
                                graph.create(nod)
                                rel = Relationship(nod, 'COM_Output_COM', node)
                            rel['report_dt'] = row[2]
                            rel['output_funt'] = row[3]
                            graph.create(rel)
            if '下游' in file:  # 下游公司
                csvpath = file_path + file
                with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                    rows = csv.reader(csvfile, delimiter=';')
                    for index, row in enumerate(rows):
                        if index == 0:
                            continue
                        rel_num += 1
                        # print(rel_num, stock_code, '-->', row)
                        if row[3] not in ['', '-', '--']:
                            row[3] = float(row[3].replace(',', ''))
                        if row[1] != '-':
                            down_code = row[1]
                            node_down = graph.find_one(label='COMPANY', property_key='stock_code', property_value=down_code)
                            if node_down:
                                rel = Relationship(node, 'COM_Output_COM', node_down)
                                rel['report_dt'] = row[2]
                                rel['output_funt'] = row[3]
                                graph.create(rel)
                            else:
                                nod = Node('COMPANY')
                                nod['com_name'] = row[0]
                                nod['stock_code'] = row[1]
                                nod['com_type'] = com_type(row[1])
                                rel = Relationship(node, 'COM_Output_COM', nod)
                                rel['report_dt'] = row[2]
                                rel['output_funt'] = row[3]
                                graph.create(nod | rel)
                        else:
                            node_down = graph.find_one(label='COMPANY', property_key='com_name', property_value=row[0])
                            if node_down:
                                rel = Relationship(node, 'COM_Output_COM', node_down)
                            else:
                                nod = Node('COMPANY')
                                nod['com_name'] = row[0]
                                nod['com_type'] = com_type(row[1])
                                graph.create(nod)
                                rel = Relationship(node, 'COM_Output_COM', nod)
                            rel['report_dt'] = row[2]
                            rel['output_funt'] = row[3]
                            graph.create(rel)
    time2 = time.time()
    print('Over: create_com_output', time2 - time1)


def create_com_invest():  # 在图中创建公司投资关系，如果公司节点不存在则创建
    time1 = time.time()
    file_path = '../Data/AStack_com_invest/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
        if node:
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                for index, row in enumerate(rows):
                    if index == 0:
                        continue
                    rel_num += 1
                    # print(rel_num, stock_code, '-->', row)
                    if row[3] not in ['', '-', '--']:
                        row[3] = float(row[3].replace(',', ''))
                    if row[1] != '-':
                        holded_code = row[1]
                        node_holded = graph.find_one(label='COMPANY', property_key='stock_code', property_value=holded_code)
                        if node_holded:
                            rel = Relationship(node, 'COM_Invest_COM', node_holded)
                            rel['report_dt'] = row[2]
                            rel['proportion'] = row[3]
                            graph.create(rel)
                        else:
                            nod = Node('COMPANY')
                            nod['com_name'] = row[0]
                            nod['stock_code'] = row[1]
                            nod['com_type'] = com_type(row[1])
                            rel = Relationship(node, 'COM_Invest_COM', nod)
                            rel['report_dt'] = row[2]
                            rel['proportion'] = row[3]
                            graph.create(nod | rel)
                    else:
                        node_holded = graph.find_one(label='COMPANY', property_key='com_name', property_value=row[0])
                        if node_holded:
                            rel = Relationship(node, 'COM_Invest_COM', node_holded)
                        else:
                            nod = Node('COMPANY')
                            nod['com_name'] = row[0]
                            nod['com_type'] = com_type(row[1])
                            graph.create(nod)
                            rel = Relationship(node, 'COM_Invest_COM', nod)
                        rel['report_dt'] = row[2]
                        rel['proportion'] = row[3]
                        graph.create(rel)
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
                ind_node = graph.find_one(label='INDUSTRY', property_key='ind_code', property_value=code)
                rel = Relationship(user_node, 'U_FocusOn_IND', ind_node)
                graph.create(rel)
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
                if not graph.find_one(label='INFORMATION', property_key='inf_id', property_value=row[0]):
                    inf_node = Node('INFORMATION')
                    inf_node['inf_id'] = row[0]
                    inf_node['inf_title'] = row[1]
                    rel = Relationship(inf_node, 'INF_ReferTo_IND', label_node)
                    graph.create(inf_node | rel)
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
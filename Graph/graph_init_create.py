from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Data_process.com_data_extraction import file_name


# graph = Con_Neo4j(http='http://0.0.0.0:7474', username='neo4j', password='Neo4j')


def Create_CompanyAndAStock():  # 在图中创建A股上市公司节点
    with open('../Data/company.csv', 'r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        count = -1
        for row in rows:
            count += 1
            if count == 0:
                continue
            node = Node('COMPANY')
            node['stock_code'] = row[0]
            node['chi_sht'] = row[1]
            node['com_name'] = row[2]
            node['eng_name'] = row[3]
            node['found_dt'] = row[4]
            node['reg_prov'] = row[5]
            node['old_nmae'] = row[6]
            node['legal_rep'] = row[7]
            node['ind_dir'] = row[8]
            node['acc_firm'] = row[9]
            node['sec_aff_rep'] = row[10]
            node['adv_ser'] = row[11]
            node['block_name'] = row[12]
            node['indu_name'] = row[13]
            graph.create(node)
            print(count, row)


def Create_Com_UpAndDown():  # 在图中创建公司产业输出关系（上下游），如果公司节点不存在则创建
    file_path = '../Data/A股上市公司上下游/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        # if stock_code[0] not in ['0', '3', '6']:
        #     continue
        node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
        if '上游' in file:  # 上游公司
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                k = -1
                for row in rows:
                    k += 1
                    if k == 0:
                        continue
                    rel_num += 1
                    print(rel_num, row, '-->', stock_code)
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
                            rel = Relationship(nod, 'COM_Output_COM', node)
                            rel['report_dt'] = row[2]
                            rel['output_funt'] = row[3]
                            graph.create(nod | rel)
                    else:
                        nod = Node('COMPANY')
                        nod['com_name'] = row[0]
                        graph.create(nod)
                        rel = Relationship(nod, 'COM_Output_COM', node)
                        rel['report_dt'] = row[2]
                        rel['output_funt'] = row[3]
                        graph.create(rel)
        if '下游' in file:  # 下游公司
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                k = -1
                for row in rows:
                    k += 1
                    if k == 0:
                        continue
                    rel_num += 1
                    print(rel_num, stock_code, '-->', row)
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
                            rel = Relationship(node, 'COM_Output_COM', nod)
                            rel['report_dt'] = row[2]
                            rel['output_funt'] = row[3]
                            graph.create(nod | rel)
                    else:
                        nod = Node('COMPANY')
                        nod['com_name'] = row[0]
                        graph.create(nod)
                        rel = Relationship(node, 'COM_Output_COM', nod)
                        rel['report_dt'] = row[2]
                        rel['output_funt'] = row[3]
                        graph.create(rel)


def Create_Industry():  # 在图中创建行业节点，以及公司与行业的关系
    with open('../Data/industry_tag.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile)
        k = -1
        for row in rows:
            k += 1
            if k == 0:
                continue
            com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=row[0])
            ind_node = graph.find_one(label='INDUSTRY', property_key='ind_name', property_value=row[2])
            if not com_node:
                print(k, row)
                continue
            if not ind_node:
                new_node = Node('INDUSTRY')
                new_node['ind_name'] = row[2]
                com_rel = Relationship(com_node, 'COM_BelongTo_I', new_node)
                graph.create(new_node | com_rel)
            else:
                com_rel = Relationship(com_node, 'COM_BelongTo_I', ind_node)
                graph.create(com_rel)


def Create_Com_Invest():  # 在图中创建公司投资关系，如果公司节点不存在则创建
    file_path = '../Data/A股上市公司投资情况/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
        if node:
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                k = -1
                for row in rows:
                    k += 1
                    if k == 0:
                        continue
                    rel_num += 1
                    print(rel_num, stock_code, '-->', row)
                    row[3] = float(row[3])
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
                            rel = Relationship(node, 'COM_Invest_COM', nod)
                            rel['report_dt'] = row[2]
                            rel['proportion'] = row[3]
                            graph.create(nod | rel)
                    else:
                        nod = Node('COMPANY')
                        nod['com_name'] = row[0]
                        graph.create(nod)
                        rel = Relationship(node, 'COM_Invest_COM', nod)
                        rel['report_dt'] = row[2]
                        rel['proportion'] = row[3]
                        graph.create(rel)

if __name__ == '__main__':
    file_path = '../Data/板块/'
    files = file_name(file_path)
    count = 0
    codes = []
    for file in files:  # 遍历文件夹中的所有的文件
        if '.csv' not in file:
            continue
        csvpath = file_path + file
        with open(csvpath, 'r', encoding='utf8', newline='') as csvfile:
            rows = csv.reader(csvfile, delimiter=';')
            k = -1
            for row in rows:
                k += 1
                if k == 0:
                    continue
                if row[1][0] not in ['0', '3', '6'] or row[1][-2:] not in ['SH', 'SZ']:
                    print(file, row)
                    continue
                elif row[1] not in codes:
                    codes.append(row[1])
                    count += 1
    print(count)
    with open('../Data/company.csv', 'r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        k = -1
        com_codes = []
        for row in rows:
            k += 1
            if k == 0:
                continue
            com_codes.append(row[0])
    for code in codes:
        if code not in com_codes:
            print(code)
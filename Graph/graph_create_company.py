from Graph.process import Con_Neo4j
from py2neo import Relationship, Node
import csv
from Company_SupplyAndDemand.com_data_extraction import file_name


graph = Con_Neo4j(http='http://0.0.0.0:7474', username='neo4j', password='Neo4j')


def creat_AStockAndCompany():  # 在图中创建A股股票节点、对应上市公司节点，以及它们之间的关系
    with open('../Data/company.csv', 'r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        count = -1
        for row in rows:
            count += 1
            if count == 0:
                continue
            node1 = Node('COMPANY')
            node2 = Node('STOCK')
            node1['stock_code'] = row[0]
            node1['chi_sht'] = row[1]
            node1['com_name'] = row[2]
            node1['eng_name'] = row[3]
            node1['found_dt'] = row[4]
            node1['reg_prov'] = row[5]
            node1['old_nmae'] = row[6]
            node1['legal_rep'] = row[7]
            node1['ind_dir'] = row[8]
            node1['acc_firm'] = row[9]
            node1['sec_aff_rep'] = row[10]
            node1['adv_ser'] = row[11]
            node1['block_name'] = row[12]
            node1['indu_name'] = row[13]
            node2['stock_code'] = row[0]
            node2['stock_name'] = row[1]
            rel = Relationship(node1, 'COM_Issue_S', node2)
            graph.create(node1 | rel | node2)
            print(count, row)


def creat_com_UpAndDown():  # 创建公司上下游关系，如果有图中不存在的公司，则创建它
    file_path = '../Company_SupplyAndDemand/上市公司上下游/'
    files = file_name(file_path)
    rel_num = 0
    for file in files:  # 遍历文件夹中的所有A股上下游的.csv文件
        if '.csv' not in file:
            continue
        stock_code = ((file.split('['))[1].split(']'))[0]
        if stock_code[0] not in ['0', '3', '6']:
            continue
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


if __name__ == '__main__':
    creat_AStockAndCompany()
    creat_com_UpAndDown()
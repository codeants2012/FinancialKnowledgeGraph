import csv
from Graph.graph_init_create import graph, file_name, re


def com_data_pre1():  # 爬取的公司数据-预处理1：调整字段顺序、删除多余字符
    with open('../Crawler_Script/data/company.csv', 'r', encoding='utf-8') as read, \
            open('../Data/company.csv', 'w', encoding='utf-8', newline='') as write:
        head = ['股票代码', '公司简称', '公司全称', '公司英文名称', '成立日期', '所属地域', '曾用名', '法定代表人', '独立董事',
                '会计师事务所', '证券事务代表', '咨询服务机构', '所属概念', '所属行业']
        rows = csv.reader(read)
        csvwriter = csv.writer(write, delimiter=';')
        csvwriter.writerow(head)
        count = 0
        for row in rows:
            if row[0][0] not in ['0', '3', '6']:
                continue
            temp_list = []
            try:
                dict = eval(row[1])
                for key in head:
                    temp_list.append(dict[key].replace('\n', '').replace('\t', '').replace('\r', ''))
                if temp_list[0] == '':
                    print('Missing:', row[0])
                else:
                    count += 1
                    csvwriter.writerow(temp_list)
            except:
                print('TypeError:', row[0])
    print(count)


def com_data_pre2():  # 爬取的公司数据-预处理2：解决部分数据字段分隔错误问题、删除多余空格、补全股票代码
    with open('../Data/com_temp.csv', 'r', encoding='utf-8', newline='') as csvfile, \
            open('../Data/company.csv', 'w', encoding='utf-8', newline='') as csv2:
        rows = csv.reader(csvfile, delimiter=';')
        csvwriter = csv.writer(csv2, delimiter=';')
        count = 0
        for row in rows:
            count += 1
            if len(row) < 14:
                data = ''
                for i in range(len(row)):
                    data = data + row[i].replace('  ', ' ')
                row = data.split(';')
            if row[0][0] == '6':
                s = row[0] + '.SH'
            elif row[0][0] == '0' or row[0][0] == '3':
                s = row[0] + '.SZ'
            else:
                s = row[0]
            temp_list = [s] + row[1:6] + row[8:14] + [row[7]] + [row[6]]
            csvwriter.writerow(temp_list)
            print(count, temp_list)


def com_data_pre3():  # 整合并排序多个文件中的公司数据
    with open('../Data/company.csv', 'r', encoding='utf-8', newline='') as csv1, \
            open('../Data/add_company.csv', 'r', encoding='utf-8', newline='') as csv2, \
            open('../Data/company1.csv', 'w', encoding='utf-8', newline='') as csv3:
        rows1 = csv.reader(csv1, delimiter=';')
        rows2 = csv.reader(csv2, delimiter=';')
        csvwriter = csv.writer(csv3, delimiter=';')
        data = []
        for index, row in enumerate(rows1):
            if index == 0:
                head = row
                continue
            data.append(row)
        for row in rows2:
            if '.' not in row[0]:
                row[0] = row[0] + ('.SH' if row[0][0] == '6' else '.ßSZ')
            data.append(row)
        csvwriter.writerow(head)
        for row in sorted(data):
            csvwriter.writerow(row)


def node_encoding():  # 编码所有节点
    with open('../Data/Encoding/node_encoding.csv', 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter='\t')
        writer.writerow(['节点类型', '原标识', '编码'])
        nodes = graph.find(label='USER')
        label = 'USER'
        for node in nodes:
            pro1 = node['user_id']
            pro2 = 'USER' + node['user_id'].zfill(12)
            writer.writerow([label, pro1, pro2])
            print(label, pro1, pro2)
        nodes = graph.find(label='COMPANY')
        label = 'COMPANY'
        k = 0
        for node in nodes:
            pro1 = node['com_name']
            if 'A' in node['com_type'] or 'B' in node['com_type']:
                pro2 = 'COMP' + (node['stock_code'].split('.'))[0].zfill(12)
            else:
                k += 1
                pro2 = 'COMP0001' + str(k).zfill(8)
            writer.writerow([label, pro1, pro2])
            print(label, pro1, pro2)
        nodes = graph.find(label='INFORMATION')
        label = 'INFORMATION'
        for node in nodes:
            pro1 = node['inf_id']
            pro2 = 'INFO' + pro1.zfill(12)
            writer.writerow([label, pro1, pro2])
            print(label, pro1, pro2)
        nodes = graph.find(label='INDUSTRY')
        label = 'INDUSTRY'
        for node in nodes:
            pro1 = node['ind_name']
            pro2 = 'INDU' + node['ind_code'].zfill(12)
            writer.writerow([label, pro1, pro2])
            print(label, pro1, pro2)
        nodes = graph.find(label='BLOCK')
        label = 'BLOCK'
        k = 0
        for node in nodes:
            pro1 = node['block_name']
            k += 1
            pro2 = 'BLOC' + str(k).zfill(12)
            writer.writerow([label, pro1, pro2])
            print(label, pro1, pro2)


def edge_formatting():  # 抽取所有边，并依照节点编码格式化
    d = dict()
    k = 0
    with open('../Data/Encoding/node_encoding.csv', 'r', encoding='utf-8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            d[row[1]] = row[2]

    with open('../Data/Encoding/six_with_edgetype.edgelist', 'w', encoding='utf-8', newline='') as edgelist:
        edgewriter = csv.writer(edgelist, delimiter='\t')
        with open('../Data/com_industry_tags.csv', 'r', encoding='utf-8', newline='') as csvfile:
            rows = csv.reader(csvfile)
            for index, row in enumerate(rows):
                if index == 0:
                    continue
                try:
                    node1 = d[row[0]]
                    node2 = d[row[2]]
                    edgetype = {'edgetype': 'COM_BelongTo_IND'}
                    edge = [node1, node2, edgetype]
                    edgewriter.writerow(edge)
                    k += 1
                    print(k, edge)
                except:
                    continue

        file_path = '../Data/AStack_com_block/'
        files = file_name(file_path)
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
                        try:
                            node1 = d[node['com_name']]
                            node2 = d[row[0]]
                            edgetype = {'edgetype': 'COM_BelongTo_B'}
                            edge = [node1, node2, edgetype]
                            edgewriter.writerow(edge)
                            k += 1
                            print(k, edge)
                        except:
                            continue

        file_path = '../Data/AStack_com_output/'
        files = file_name(file_path)
        for file in files:  # 遍历文件夹中的所有的文件
            if '.csv' not in file:
                continue
            stock_code = ((file.split('['))[1].split(']'))[0]
            node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock_code)
            if node:
                if '上游' in file:  # 上游公司
                    csvpath = file_path + file
                    with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                        rows = csv.reader(csvfile, delimiter=';')
                        for index, row in enumerate(rows):
                            if index == 0:
                                continue
                            try:
                                node1 = d[row[0]]
                                node2 = d[node['com_name']]
                                edgetype = {'edgetype': 'COM_Output_COM'}
                                edge = [node1, node2, edgetype]
                                edgewriter.writerow(edge)
                                k += 1
                                print(k, edge)
                            except:
                                continue
                if '下游' in file:  # 下游公司
                    csvpath = file_path + file
                    with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                        rows = csv.reader(csvfile, delimiter=';')
                        for index, row in enumerate(rows):
                            if index == 0:
                                continue
                            try:
                                node1 = d[node['com_name']]
                                node2 = d[row[0]]
                                edgetype = {'edgetype': 'COM_Output_COM'}
                                edge = [node1, node2, edgetype]
                                edgewriter.writerow(edge)
                                k += 1
                                print(k, edge)
                            except:
                                continue

        file_path = '../Data/AStack_com_invest/'
        files = file_name(file_path)
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
                        try:
                            node1 = d[node['com_name']]
                            node2 = d[row[0]]
                            edgetype = {'edgetype': 'COM_Invest_COM'}
                            edge = [node1, node2, edgetype]
                            edgewriter.writerow(edge)
                            k += 1
                            print(k, edge)
                        except:
                            continue

        with open('../Data/User/user_labels_ind.txt', mode='r', encoding='utf-8', newline='') as txtfile:
            rows = txtfile.readlines()
            for row in rows:
                pattern = re.compile(r'\d+')
                res = re.findall(pattern, row)
                node1 = d[res[0]]
                codes = res[1:]
                for code in codes:
                    node2 = d[code]
                    edgetype = {'edgetype': 'U_FocusOn_IND'}
                    edge = [node1, node2, edgetype]
                    edgewriter.writerow(edge)
                    k += 1
                    print(k, edge)

        file_path = '../Data/Information/inf_labels/'
        files = file_name(file_path)
        for file in files:  # 遍历文件夹中的所有的文件
            if '.csv' not in file:
                continue
            label_name = (file.split('.'))[0]
            csvpath = file_path + file
            with open(csvpath, 'r', encoding='utf-8', newline='') as csvfile:
                rows = csv.reader(csvfile, delimiter=';')
                for index, row in enumerate(rows):
                    if index == 0:
                        continue
                    try:
                        node1 = d[row[0]]
                        node2 = d[label_name]
                        edgetype = {'edgetype': 'INF_ReferTo_IND'}
                        edge = [node1, node2, edgetype]
                        edgewriter.writerow(edge)
                        k += 1
                        print(k, edge)
                    except:
                        continue


def edgelist_process():  # 边列表处理
    with open('../Data/Encoding/six_with_edgetype.edgelist', 'r', encoding='utf-8', newline='') as edgelist1, \
            open('../Data/Encoding/six.edgelist', 'w', encoding='utf-8', newline='') as edgelist2:
        edgewreader = csv.reader(edgelist1, delimiter='\t')
        edgewriter = csv.writer(edgelist2, delimiter='\t')
        for row in edgewreader:
            edgewriter.writerow(row[0:2])


if __name__ == '__main__':
    edgelist_process()

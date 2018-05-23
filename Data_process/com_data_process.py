import csv


def com_data_pre1():  # 爬取的公司数据-预处理1：调整字段顺序、删除多余字符
    with open('../Crawler_Script/data/company.csv', 'r', encoding='utf8') as read, \
            open('../Data/company.csv', 'w', encoding='utf8', newline='') as write:
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
    with open('../Data/com_temp.csv', 'r', encoding='utf8', newline='') as csvfile, \
            open('../Data/company.csv', 'w', encoding='utf8', newline='') as csv2:
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
    with open('../Data/company.csv', 'r', encoding='utf8', newline='') as csv1, \
            open('../Data/add_company.csv', 'r', encoding='utf8', newline='') as csv2, \
            open('../Data/company1.csv', 'w', encoding='utf8', newline='') as csv3:
        rows1 = csv.reader(csv1, delimiter=';')
        rows2 = csv.reader(csv2, delimiter=';')
        csvwriter = csv.writer(csv3, delimiter=';')
        data = []
        k = -1
        for row in rows1:
            k += 1
            if k == 0:
                head = row
                continue
            data.append(row)
        for row in rows2:
            if '.' not in row[0]:
                row[0] = row[0] + ('.SH' if row[0][0] == '6' else ',SZ')
            data.append(row)
        csvwriter.writerow(head)
        for row in sorted(data):
            csvwriter.writerow(row)


if __name__ == '__main__':
    com_data_pre3()
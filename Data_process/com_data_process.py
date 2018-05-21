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
            list = []
            try:
                dict = eval(row[1])
                for key in head:
                    list.append(dict[key].replace('\n', '').replace('\t', '').replace('\r', ''))
                if list[0] == '':
                    print('Missing:', row[0])
                else:
                    count += 1
                    csvwriter.writerow(list)
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
            if (len(row) < 14):
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
            l = [s] + row[1:6] + row[8:14] + [row[7]] + [row[6]]
            csvwriter.writerow(l)
            print(count, l)


if __name__ == '__main__':
    com_data_pre1()
    com_data_pre2()

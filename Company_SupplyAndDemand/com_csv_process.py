import csv

if __name__ == '__main__':
    # with open('../Crawler_Script/data/company.csv', 'r', encoding='utf8') as read, open('../Data/company.csv', 'w',
    #                                                                                     encoding='utf8',
    #                                                                                     newline='') as write:
    #     head = ['股票代码', '公司简称', '公司全称', '公司英文名称', '成立日期', '所属地域', '所属行业', '所属概念', '曾用名', '法定代表人', '独立董事', '会计师事务所',
    #             '证券事务代表', '咨询服务机构']
    #     rows = csv.reader(read)
    #     csvwriter = csv.writer(write, delimiter=';')
    #     csvwriter.writerow(head)
    #     count = 0
    #     for row in rows:
    #         if row[0][0] == '2' or row[0][0] == '9':
    #             continue
    #         list = []
    #         try:
    #             dict = eval(row[1])
    #             list.append(dict['股票代码'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['公司简称'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['公司全称'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['公司英文名称'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['成立日期'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['所属地域'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['所属行业'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['所属概念'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['曾用名'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['法定代表人'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['独立董事'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['会计师事务所'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['证券事务代表'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             list.append(dict['咨询服务机构'].replace('\n', '').replace('\t', '').replace('\r', ''))
    #             if list[0] == '':
    #                 print(row[0])
    #             else:
    #                 count += 1
    #                 csvwriter.writerow(list)
    #         except:
    #             print(row[0])
    # print(count)

    with open('../Data/com_temp.csv', 'r', encoding='utf8', newline='') as csvfile, open('../Data/company.csv', 'w',
                                                                                         encoding='utf8',
                                                                                         newline='') as csv2:
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
            l = row[0:6] + row[8:14] + [row[7]] + [row[6]]
            csvwriter.writerow(l)
            for ls in l:
                if ';' in ls:
                    print(count, l)

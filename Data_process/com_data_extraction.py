import csv
import os

from xlrd import open_workbook


def file_name(file_dir):  # 获取文件夹信息
    for root, dirs, files in os.walk(file_dir):
        # print(root) #当前目录路径
        # print(dirs) #当前路径下所有子目录
        # print(files) #当前路径下所有非目录子文件
        return files  # 返回文件列表


def find_missing_files(files, xlspath):  # 查找遗漏的文件项
    workbook = open_workbook(xlspath)
    booksheet = workbook.sheet_by_index(0)
    rows = booksheet.nrows
    for i in range(rows):
        code = booksheet.cell_value(i, 0)
        key = 0
        for file in files:
            if code in file:
                key = 1
                break
            else:
                continue
        if key == 0:
            print(code)


def com_output_extraction():  # 将Excel文件中的公司上下游数据抽取到csv文件中
    file_path = '../Data/A股上市公司关系网/'
    files = file_name(file_path)
    count = 0
    for file in files:
        if '.xlsx' not in file or file[0] == '~':
            continue
        fpath = file_path + file
        csvpath = '../Data/A股上市公司上下游/'
        workbook = open_workbook(fpath)
        if '上游' in workbook.sheet_names():
            booksheet = workbook.sheet_by_name('上游')
            rows = booksheet.nrows
            if booksheet.cell_value(2, 0) == '供应商' and rows >= 6:
                count += 1
                rows = booksheet.nrows
                name = booksheet.cell_value(0, 0)
                print(count, name)
                head = []
                for i in range(4):
                    head.append(str(booksheet.cell_value(4, i)))
                path = csvpath + name + '.csv'
                with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=';')
                    csvwriter.writerow(head)
                    for row in range(5, rows):
                        if booksheet.cell_value(row, 0) == '应付账款':
                            break
                        if booksheet.cell_value(row, 0) == '':
                            continue
                        cell_list = []
                        for i in range(4):
                            cell_list.append(str(booksheet.cell_value(row, i)))
                        csvwriter.writerow(cell_list)
        if '下游' in workbook.sheet_names():
            booksheet = workbook.sheet_by_name('下游')
            rows = booksheet.nrows
            if booksheet.cell_value(2, 0) == '客户' and rows >= 6:
                count += 1
                name = booksheet.cell_value(0, 0)
                print(count, name)
                head = []
                for i in range(4):
                    head.append(str(booksheet.cell_value(4, i)))
                path = csvpath + name + '.csv'
                with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=';')
                    csvwriter.writerow(head)
                    for row in range(5, rows):
                        if booksheet.cell_value(row, 0) == '应收账款':
                            break
                        if booksheet.cell_value(row, 0) == '':
                            continue
                        cell_list = []
                        for i in range(4):
                            cell_list.append(str(booksheet.cell_value(row, i)))
                        csvwriter.writerow(cell_list)


def com_invest_extraction():  # 将Excel文件中的公司投资数据抽取到csv文件中
    file_path = '../Data/A股上市公司关系网/'
    files = file_name(file_path)
    count = 0
    for file in files:
        if '.xlsx' not in file or file[0] == '~':
            continue
        fpath = file_path + file
        csvpath = '../Data/A股上市公司投资情况/'
        workbook = open_workbook(fpath)
        if '投资' in workbook.sheet_names():
            booksheet = workbook.sheet_by_name('投资')
            rows = booksheet.nrows
            if booksheet.cell_value(2, 0) == '控参股' and rows >= 6:
                count += 1
                rows = booksheet.nrows
                name = booksheet.cell_value(0, 0)
                print(count, name)
                head = []
                for i in range(4):
                    head.append(str(booksheet.cell_value(4, i)))
                path = csvpath + name + '.csv'
                with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=';')
                    csvwriter.writerow(head)
                    for row in range(5, rows):
                        if booksheet.cell_value(row, 0) == '投资PEVC基金':
                            break
                        if booksheet.cell_value(row, 0) == '':
                            continue
                        cell_list = []
                        for i in range(4):
                            cell_list.append(str(booksheet.cell_value(row, i)))
                        csvwriter.writerow(cell_list)


def com_block_extraction():  # 将Excel文件中的所有板块数据抽取到csv文件中
    file_path = '../Data/A股上市公司关系网/'
    files = file_name(file_path)
    blocks = []
    head = ['公司名称', '股票代码', '营业总收入(万元)']
    count = 0
    for file in files:
        if '.xlsx' not in file or file[0] == '~':
            continue
        fpath = file_path + file
        csvpath = '../Data/板块/'
        workbook = open_workbook(fpath)
        if '概念' in workbook.sheet_names():
            booksheet = workbook.sheet_by_name('概念')
            rows = booksheet.nrows
            if booksheet.cell_value(2, 0) != '' and rows >= 5:
                rows = booksheet.nrows
                name = booksheet.cell_value(0, 0)[0:-2] + '板块'
                com_blocks = []
                datas = []
                block_com = ''
                for row in range(2, rows):
                    if booksheet.cell_value(row, 0) == '':
                        if block_com not in blocks:
                            blocks.append(block_com)
                            path = csvpath + block_com + '.csv'
                            with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                                csvwriter = csv.writer(csvfile, delimiter=';')
                                csvwriter.writerow(head)
                                k = -1
                                for data in datas:
                                    k += 1
                                    if k == 0:
                                        continue
                                    csvwriter.writerow(data)
                        datas = []
                        continue
                    elif booksheet.cell_value(row, 1) == '':
                        block_com = booksheet.cell_value(row, 0).replace('/', '_')
                        if block_com not in com_blocks:
                            com_blocks.append(block_com)
                    else:
                        cell_list = []
                        for i in range(3):
                            cell_list.append(str(booksheet.cell_value(row, i)))
                        datas.append(cell_list)
                if block_com not in blocks:
                    blocks.append(block_com)
                    path = csvpath + block_com + '.csv'
                    with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile, delimiter=';')
                        csvwriter.writerow(head)
                        k = -1
                        for data in datas:
                            k += 1
                            if k == 0:
                                continue
                            csvwriter.writerow(data)
                path = '../Data/A股上市公司所属板块/' + name + '.csv'
                with open(file=path, mode='w', encoding='utf-8', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=';')
                    csvwriter.writerow(['板块名称'])
                    for b in com_blocks:
                        csvwriter.writerow([b])
                count += 1
                print(count, name, com_blocks)


if __name__ == '__main__':
    com_output_extraction()

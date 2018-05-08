from xlrd import open_workbook
import os
import csv


def file_name(file_dir):
    for root, dirs, files in os.walk(file_dir):
        # print(root) #当前目录路径
        # print(dirs) #当前路径下所有子目录
        # print(files) #当前路径下所有非目录子文件
        return files


def find_missing(files, xlspath):
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


def xls_to_csv(fpath, csvpath):
    workbook = open_workbook(fpath)
    if '上游' in workbook.sheet_names():
        booksheet = workbook.sheet_by_name('上游')
        if booksheet.cell_value(2, 0) == '供应商':
            rows = booksheet.nrows
            name = booksheet.cell_value(0, 0)
            print(name)
            head = []
            for i in range(4):
                head.append(str(booksheet.cell_value(4, i)))
            path = csvpath + name + '.csv'
            with open(file=path, mode='w+', encoding='utf8', newline='') as csvfile:
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
        if booksheet.cell_value(2, 0) == '客户':
            rows = booksheet.nrows
            name = booksheet.cell_value(0, 0)
            print(name)
            head = []
            for i in range(4):
                head.append(str(booksheet.cell_value(4, i)))
            path = csvpath + name + '.csv'
            with open(file=path, mode='w+', encoding='utf8', newline='') as csvfile:
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


if __name__ == '__main__':
    file_path = './上市公司关系网/'
    files = file_name(file_path)
    count = 0
    for file in files:
        count += 1
        print(count, file)
        fpath = file_path + file
        csvpath = './上市公司上下游/'
        xls_to_csv(fpath, csvpath)

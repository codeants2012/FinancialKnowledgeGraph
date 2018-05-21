import csv


if __name__ == '__main__':
    with open('Data/sec_tags1.csv', 'r', encoding='utf-8', newline='') as csv1, open('Data/sec_tags.csv', 'w', encoding='utf-8', newline='') as csv2:
        rows = csv.reader(csv1)
        writer = csv.writer(csv2)
        for row in rows:
            if row[0] == '':
                continue
            writer.writerow(row[0:3])

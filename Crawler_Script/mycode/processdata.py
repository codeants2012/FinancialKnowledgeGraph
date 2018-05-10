import csv

data = []

with open('../data/GuPiao.csv', 'r', newline='') as f:
    csv_reader = csv.reader(f)
    for line in csv_reader:
        data.append(line)

with open('../data/temp.csv', 'w', newline='') as f:
    csv_writer = csv.writer(f)
    for it in data:
        csv_writer.writerow([it[1]])

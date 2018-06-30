import csv
from urllib.request import urlopen

from bs4 import BeautifulSoup

if __name__ == '__main__':
    data = []
    for i in range(1, 72):
        url = 'http://datainfo.stock.hexun.com/ssgs/jbsj/zhyl.aspx?type=&tag=1&page=' + str(i)
        while True:
            try:
                html = urlopen(url)
                soup = BeautifulSoup(html.read(), 'html.parser')
                trs = soup.find_all(attrs={"xmlns:ms": "urn:schemas-microsoft-com:xslt"})
                break
            except:
                continue
        temp = []
        for tr in trs:
            tds = tr.find_all('td')
            name = tds[0].get_text(strip=True)
            id = tds[1].get_text(strip=True)
            temp.append((name, id))
        with open('../data/GuPiao.csv', 'a', newline='') as f:
            csv_writer = csv.writer(f)
            for it in temp:
                csv_writer.writerow(list(it))
        data += temp
        print('finish page', i)

    data_set = set()
    for it in data:
        data_set.add(it)

    print('total number', len(data_set))

    # with open('../data/GuPiao.csv', 'w', newline='') as f:
    #     csv_writer = csv.writer(f)
    #     for it in data_set:
    #         csv_writer.writerow(list(it))

from Graph.process import Con_Neo4j
import csv

graph = Con_Neo4j(http='http://0.0.0.0:7474', username='neo4j', password='Neo4j')


def creat_StockAndCompany():  # 在图中创建股票节点、上市公司节点，以及它们之间的关系
    with open('../Data/company.csv', 'r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=';')
        count = -1
        for row in rows:
            count += 1
            if count == 0:
                continue
            graph.run(f'create (a:STOCK),(b:COMPANY) with a,b set a.stock_code="{row[0]}",a.stock_name="{row[1]}",'
                      f'b.stock_code="{row[0]}",b.chi_sht="{row[1]}",b.com_name="{row[2]}",b.eng_name="{row[3]}",'
                      f'b.found_dt="{row[4]}",b.reg_prov="{row[5]}",b.old_nmae="{row[6]}",b.legal_rep="{row[7]}",'
                      f'b.ind_dir="{row[8]}",b.acc_firm="{row[9]}",b.sec_aff_rep="{row[10]}",b.adv_ser="{row[11]}",'
                      f'b.concept_name="{row[12]}",b.indu_name="{row[13]}" with a,b create (b)-[:COM_Issue_S]->(a)')
            print(count, row)


# def creat_com_UpAndDown():  # 创建公司上下游关系



if __name__ == '__main__':
    creat_StockAndCompany()
from py2neo import Graph, Node, Relationship, walk

# 连接图数据库
graph = Graph('http://127.0.0.1:7474', username='neo4j', password='123456', bolt=True, secure=False)

# node1 = Node('USER')
# node1['name'] = '1'
# node2 = Node('USER')
# node2['name'] = '2'
# rel = Relationship(node1, 'LINK', node2)
# rel['rel_name'] = '1-2'
# graph.merge(rel | node1)

node1 = graph.find_one(label='USER', property_key='name', property_value='1')
node2 = graph.find_one(label='USER', property_key='name', property_value='2')
rel = graph.match_one(node1, 'LINK', node2)
rel['id'] = 'X12'
node1['id'] = None
graph.push(rel)
graph.pull(rel)
for i in walk(rel):
    print(i.labels())

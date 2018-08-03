from py2neo import Graph, Node, Relationship, walk, types, Walkable

# 连接图数据库
graph = Graph('http://127.0.0.1:7474', username='neo4j', password='123456', bolt=True, secure=False)

# node1 = Node('USER')
# node1['name'] = '1'
# node2 = Node('USER')
# node2['name'] = '2'
# rel = Relationship(node1, 'LINK', node2)
# rel['rel_name'] = '1-2'
# graph.merge(rel | node1)

# node1 = graph.find_one(label='USER', property_key='name', property_value='1')
# node2 = graph.find_one(label='USER', property_key='name', property_value='2')
# rel = graph.match_one(node1, 'LINK', node2)
# rel['id'] = 'X12'
# node1['id'] = None
# graph.push(rel)
# graph.pull(rel)
# for i in walk(rel):
#     print(i.labels())

id1 = '1'
node1 = graph.data(f"match (n:USER) where n.name='{id1}' return n")
# print(node1)
if node1:
    # node = node1[0]['n']
    # print(node)
    # node['pid'] = '111'
    # graph.push(node)
    # rel = graph.data(f"match {node}-[r]-(n) return n,r")
    # if rel:
    #     print(rel)
    #     print(rel[0]['n'])
    #     print(rel[0]['r'])
        # rel[0]['n']['pid'] = '222'
        # rel[0]['r']['pid'] = '1122'
        # graph.push(rel[0]['r'])
    paths = graph.data(f"match (n:USER) where n.name='{id1}' call apoc.path.expand(n,'','',1,10) yield path return path")
    for path in paths:
        # print(path)
        p = path['path']
        n_p = walk(p)
        print(p.start_node()['name'], list(p.end_node().labels())[0], p.nodes()[0].labels())
        for x in n_p:
            print(type(x) is types.Relationship, x)

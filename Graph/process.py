import time

import pymysql
from py2neo import Graph


def Con_Neo4j(http=None, username=None, password=None):
    """
    :param http: 数据库地址
    :param username: 该数据库的使用者
    :param password: 密码
    :return: 返回数据库链接
    """
    connection = Graph(http, user=username, password=password, bolt=True)  # 连接图数据库
    print('Neo4j Connect successful!')
    return connection


def Con_MySQL(database=None, user=None, password=None, host=None, port=3306, charset="utf8"):
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    connMy = pymysql.connect(db=database, user=user, passwd=password, host=host, port=port, charset=charset)
    print("MySQL Connect successful!")
    return connMy


def Period_Cmp(start_time1=None, end_time1=None, start_time2=None, end_time2=None):
    """
    函数功能：匹配时间段重叠情况。
    时间格式统一为‘1995-4-16’。
    :param start_time1: 开始时间1
    :param end_time1: 结束时间1
    :param start_time2: 开始时间2
    :param end_time2: 结束时间2
    :return: 重叠时间段及重叠年数（最小为1），或False表示不重叠
    """
    start_t1 = start_time1.split('-')
    end_t1 = end_time1.split('-')
    start_t2 = start_time2.split('-')
    end_t2 = end_time2.split('-')
    time_int = [365 * (int(start_t1[0]) - 1) + 30 * (int(start_t1[1]) - 1) + int(start_t1[2]),
                365 * (int(end_t1[0]) - 1) + 30 * (int(end_t1[1]) - 1) + int(end_t1[2]),
                365 * (int(start_t2[0]) - 1) + 30 * (int(start_t2[1]) - 1) + int(start_t2[2]),
                365 * (int(end_t2[0]) - 1) + 30 * (int(end_t2[1]) - 1) + int(end_t2[2])]
    overlap = abs(int(start_t1[0]) - int(start_t2[0]))  # 时间段重叠年数
    if time_int[0] <= time_int[2] <= time_int[1] or time_int[0] <= time_int[3] <= time_int[1]:
        return [start_time1 if time_int[0] >= time_int[2] else start_time2,
                end_time1 if time_int[3] >= time_int[1] else end_time2,
                1 if overlap <= 1 else overlap]  # 若存在时间重叠，则返回重叠时间段及重叠年数
    else:
        return None  # 若不存在时间重叠，则返回空


def Time_Now(type_int=0):
    """
    函数功能：将当前时间调整为特定格式。
    时间格式统一为‘1995-4-16’。
    :param type_int: 时间类型，0：'1995-4-16'，1：'Sun Apr 16 6:6:6 1995'
    :return: 请求的格式化时间
    """
    if type_int == 0:
        return str(time.localtime(time.time()).tm_year) + '-' + str(time.localtime(time.time()).tm_mon) + '-' \
               + str(time.localtime(time.time()).tm_mday)
    if type_int == 1:
        return time.asctime(time.localtime(time.time()))


if __name__ == '__main__':
    print('Hello World!')

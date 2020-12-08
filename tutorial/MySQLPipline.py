from pymysql import connect


class MySQLPipeline(object):
    def __init__(self):
        self.connect = connect(
            host='39.100.77.143',
            port=3306,
            db='jobs',
            user='root',
            passwd='mysql',
            charset='utf8',
            use_unicode=True)
        # 连接数据库
        self.cursor = self.connect.cursor()
        # 使用cursor()方法获取操作游标

    def process_item(self, item, spider):
        insert_sql = """
        insert into fivejob(address, salary, create_time, body, company_name, postion_id, position_name, work_year, educational) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        self.cursor.execute(
            insert_sql,
(            item['address'],
            item['salary'],
            item['create_time'],
            ' '.join(item['body']),
            item['company_name'],
            item['postion_id'],
            item['position_name'],
            item['work_year'],
            item['educational'],
)
            )
        # 执行sql语句，item里面定义的字段和表字段一一对应
        self.connect.commit()
        # 提交
        return item
    # 返回item

    def close_spider(self, spider):
        self.cursor.close()
        # 关闭游标
        self.connect.close()
        # 关闭数据库连接
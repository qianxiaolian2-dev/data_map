import pymysql

# ============ 连接配置（请自行填写） ============
DB_CONFIG = {
    "host": "rm-uf6hzomcjxkhwx56qxo.mysql.rds.aliyuncs.com",         # 数据库地址
    "port": 3306,       # 端口
    "user": "root",         # 用户名
    "password": "FTyNnEm5JsKCyR3nyeDk",     # 密码
    "database": "fdldb",    # 留空可查询所有库，也可指定具体库名
    "charset": "utf8mb4",
}


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def query(sql, params=None):
    """执行查询，返回所有结果行"""
    conn = get_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def execute(sql, params=None):
    """执行写操作（INSERT/UPDATE/DELETE）"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            affected = cur.execute(sql, params)
            conn.commit()
            return affected
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        conn = get_connection()
        print("连接成功！")
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            print(f"MySQL 版本: {cur.fetchone()[0]}\n")

            cur.execute("SHOW DATABASES")
            databases = cur.fetchall()
            print(f"共 {len(databases)} 个数据库：")
            for db in databases:
                print(f"  - {db[0]}")
        conn.close()
    except Exception as e:
        print(f"连接失败: {e}")

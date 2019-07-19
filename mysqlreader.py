import pymysql
import re
import argparse


def convert_index_ddl(table_name,file_name):
    re_key_1 = re.compile(r'KEY `(\w+)` \((.*)\)')
    re_key_2 = re.compile(r'UNIQUE KEY `(\w+)` \((.*)\)')
    re_key_3 = re.compile(r'PRIMARY KEY +\((.*)\)')
    cursor.execute("show create table %s" % table_name)
    rows = cursor.fetchall()
    ddl = rows[0][1]
    i_ddl = ""
    for line in ddl.split("\n"):
        # 一般索引
        key_match = re_key_1.search(line)
        pkey_match = re_key_3.search(line)
        if key_match:
            key_name = key_match.group(1)
            key_props = key_match.group(2).replace("`", "")
            if "UNIQUE" in line:
                i_ddl += "alter table " + table_name + " add constraint " + key_name + " unique (" + key_props + ");\n"
            else:
                i_ddl += "create index " +key_name + " on " + table_name + "(" \
                         + key_props.replace("`", "") + ");\n"
        elif pkey_match:
            key_props = pkey_match.string.replace("`", "").replace(","," ")
            i_ddl += "alter table " + table_name + " add constraint P_" + table_name[2:] + key_props +";\n"

    f = open("out/%s.tab" % file_name, "a")
    f.write(i_ddl)
    f.close()


def convert_table_ddl(table_name,file_name):
    cursor.execute("show full columns from %s" % table_name)
    rows = cursor.fetchall()
    f = open("out/%s.tab" % file_name, "a")
    f.write("\n----------------------"+table_name+"----------------------------------\n")
    f.write("create table %s (\n" % table_name)
    i = 0
    d_def = ""
    for row in rows:
        col_name = row[0]
        col_def = row[1]
        col_nullable = row[3]
        col_default = row[5]
        if col_def.startswith("int", 0, 3):
            d_def = "number(10)"
        elif col_def.startswith("bigint", 0, 6):
            d_def = "number(20)"
        elif col_def.startswith("varchar", 0, 7):
            temp = int(col_def[7:].replace("(", "").replace(")", ""))
            if temp * 3 <= 1024:
                d_length = int(col_def[7:].replace("(", "").replace(")", "")) * 3
            elif temp > 4000:
                d_length = 4000
                d_length = int(col_def[7:].replace("(", "").replace(")", ""))
            d_def = "varchar2(" + str(d_length) + ")"
        elif col_def.startswith("char", 0, 4):
            temp = int(col_def[4:].replace("(", "").replace(")", ""))
            d_length = int(col_def[4:].replace("(", "").replace(")", ""))
            d_def = "char(" + str(d_length) + ")"
        elif col_def == "blob" or col_def == "mediumblob" or col_def == "longblob":
            d_def = "blob"
        elif col_def.startswith("decimal"):
            d_def = "number" + col_def[7:]
        elif col_def.startswith("double"):
            d_def = "number" + col_def[6:]
        elif col_def.startswith("float"):
            d_def = "number" + col_def[5:]
        elif col_def.startswith("date") or col_def.startswith("datetime") or col_def.startswith("timestamp"):
            d_def = "date"
        elif col_def.startswith("tinyint"):
            d_def = "number(4)"
        elif col_def in ("longtext", "text"):
            d_def = "clob"
        elif col_def.startswith("bit"):
            d_def = "number(1)"
        else:
            print("暂不支持该类型转换，请等待工具升级")
            print(table_name + ":" + col_def)
        line = col_name + " " + d_def
        if col_default is not None:
            if col_default == "CURRENT_TIMESTAMP":
                line += " default sysdate"
            elif col_def.startswith("date") or col_def.startswith("datetime") or col_def.startswith("timestamp"):
                line += " default to_date('" + col_default + "', 'yyyy-mm-dd hh24:mi:ss')"
            # Oracle会将空字符串自动变成null，因此不允许出现default ''，这里特殊处理
            elif col_default == "":
                line += ""
            elif d_def.startswith("number"):
                line += " default " + col_default
            else:
                line += " default '" + col_default + "'"
        if col_nullable == "NO":
            line += " not null"
        i += 1
        if i < len(rows):
            line += ","
        f.write(line + "\n")
    f.write(");\n")
    f.close()

def convert_col_comment_ddl(table_name,file_name):
    cursor.execute("show full columns from %s" % table_name)
    rows = cursor.fetchall()

    comments = ""
    for row in rows:
        col_name = row[0]
        col_comment = row[8]
        if col_comment != "":
            comments+="COMMENT ON COLUMN "+table_name+"."+col_name+" IS '"+col_comment+"';\n"
    f = open("out/%s.tab" % file_name, "a")
    f.write(comments)
    f.close()
    return
def conver_table_comment_ddl(file_name):
    cursor.execute("SHOW TABLE STATUS")
    rows = cursor.fetchall()
    tbl_comms = ""
    for row in rows:
        tbl_name = row[0]
        tbl_comment = row[-1]
        if len(tbl_comment) !=0:
            tbl_comms+="COMMENT ON TABLE " + tbl_name + " IS " +tbl_comment+"\n"

    f = open("out/%s.tab" % file_name, "a")
    f.write("\n----------------------添加表注释---------------------------------\n")
    f.write(tbl_comms)
    f.write("\n----------------------添加索引---------------------------------\n")
    f.close()
def trunFile(file_name):
    f = open("out/%s.tab" % file_name, "w")
    f.write("----mysql转oracle建表语句\n")
    f.close()
def get_tables():
    cursor.execute("show tables")
    tables = cursor.fetchall()
    file_name = "mysql2oracle"
    trunFile(file_name)
    for table in tables:
        convert_table_ddl(table[0],file_name)
        convert_col_comment_ddl(table[0],file_name)
    ##表注释
    conver_table_comment_ddl(file_name)
    ##索引
    for table in tables:
        convert_index_ddl(table[0], file_name)

def _argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--host", help="键入host地址")
    parser.add_argument("-u", "--user", help="type user name")
    parser.add_argument("-p", "--passwd", help="type password")
    parser.add_argument("-d", "--db", help="type db name")
    args = parser.parse_args()
    return args


def main():
    args = _argparse()
    host = args.host
    user = args.user
    password = args.passwd
    database = args.db
    conn = pymysql.connect(host=host, user=user, password=password, db=database)
    global cursor
    cursor = conn.cursor()
    get_tables()


if __name__ == '__main__':
    main()

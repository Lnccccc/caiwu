import pandas as pd
from datetime import datetime
import pymysql

def Get_data(filename='test1.csv'):
    cf = open(filename, encoding='gb18030', errors='ignore')
    df = pd.read_csv(cf)
    raw = df[
        ['项目编号', '项目名称', '任务编号', '任务名称', '事务处理日期', '项目支出类型', '支出数量', '项目负担成本', 'DR', 'CR', '事务处理来源', '模块连接', '接收事务处理ID',
         '发票编号', '事务处理类型', '物料编码', '物料说明', '物料类型', '任务开始时间', '采购订单号', 'LIS2', 'LIS3', 'LAST_UPDATE_DATE', '生成资产行金额',
         '已转资金额']]
    target1 = raw['生成资产行金额']-raw['已转资金额']
    target2 = raw['项目负担成本']-raw['生成资产行金额']
    target3 = raw['项目负担成本']/raw['支出数量']
    target4 = target2/target3
    raw['生成资产行金额-已转资金额'] = target1
    raw['未转资金额（按生成资产行计算)'] = target2
    raw['领用物资单价'] = target3
    raw['倒算未转资数量'] = target4
    time_stamp = datetime.now()
    time_list = []
    time_list2 = []
    for i in raw['事务处理日期']:
        time_list2.append((datetime.strptime(str(i).split(' ')[0],'%Y/%m/%d')-time_stamp).days)
        time_list.append(time_stamp.strftime('%Y/%m/%d'))
    import_time = pd.Series(data=time_list)
    time_delay = pd.Series(data=time_list2)
    raw['数据导入日期'] = import_time
    raw['未转资时间'] = time_delay
    return raw

def WriteMysql(data):
    conn = pymysql.connect(host='gz-cdb-3p82wwqf.sql.tencentcdb.com', port=62982, user='root', passwd='pjq_XXX_1022',
                           db='keyword',charset='utf8')
    cursor = conn.cursor()
    print("连接数据库成功")
    try:
        cursor.execute(
            '''CREATE TABLE caiwu_ceshi (项目编号 VARCHAR(100),项目名称 VARCHAR(100),任务编号 VARCHAR(50),任务名称 VARCHAR(100),事务处理日期 VARCHAR(50),项目支出类型 VARCHAR(50), 支出数量 INT,项目负担成本 VARCHAR(100),DR VARCHAR(100),CR VARCHAR(100) ,事务处理来源 VARCHAR(100),模块连接 VARCHAR(100),接收事务处理ID VARCHAR(100),发票编号 VARCHAR(100),事务处理类型 VARCHAR(100),物料编码 VARCHAR(100),物料说明 VARCHAR(100),物料类型 VARCHAR(100),任务开始时间 VARCHAR(100),采购订单号 VARCHAR(100),LIS2 VARCHAR(100),LIS3 VARCHAR(100),LAST_UPDATE_DATE VARCHAR(100),生成资产行金额 VARCHAR(100),已转资金额 VARCHAR(100),生成资产行金额_已转资金额 VARCHAR(100),未转资金额_按生成资产行计算 VARCHAR(100),领用物资单价 VARCHAR(100),倒算未转资数量 VARCHAR(100),数据导入日期 VARCHAR(100),未转资时间 VARCHAR(100));''')
    except:
        print('表已存在')
    for i in range(len(data['项目编号'])):
        cursor.execute('''
        INSERT INTO caiwu_ceshi VALUES('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s')
        ''' % (data[data.columns[0]][i],data[data.columns[1]][i],data[data.columns[2]][i],data[data.columns[3]][i],data[data.columns[4]][i],data[data.columns[5]][i],
               data[data.columns[6]][i],data[data.columns[7]][i],data[data.columns[8]][i],data[data.columns[9]][i],data[data.columns[10]][i],
               data[data.columns[11]][i],data[data.columns[12]][i],data[data.columns[13]][i],data[data.columns[14]][i],data[data.columns[15]][i],
               data[data.columns[16]][i],data[data.columns[17]][i],data[data.columns[18]][i],data[data.columns[19]][i],data[data.columns[20]][i],
               data[data.columns[21]][i],data[data.columns[22]][i],data[data.columns[23]][i],data[data.columns[24]][i],data[data.columns[25]][i],
               data[data.columns[26]][i],data[data.columns[27]][i],data[data.columns[28]][i],data[data.columns[29]][i],data[data.columns[30]][i]))
#31
    cursor.close()
    conn.commit()
    conn.close()
data = Get_data()
WriteMysql(data)
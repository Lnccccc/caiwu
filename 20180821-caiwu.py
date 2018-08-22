from tkinter import *
from datetime import *
from tkinter.filedialog import askopenfilename
import pandas as pd #导入包
#root2 = Tk()
selected_file = askopenfilename(filetypes=[('csv', '*.csv')])
#root2.destroy()
cf=open(selected_file,encoding='gb18030',errors='ignore')   #错误代码解决方案
df= pd.read_csv(cf) #读取文件
list_township = df[['项目编号','项目名称','任务编号','任务名称','事务处理日期','项目支出类型','支出数量','项目负担成本','DR','CR','事务处理来源','模块连接','接收事务处理ID','发票编号','事务处理类型','物料编码','物料说明','物料类型','任务开始时间','采购订单号','LIS2','LIS3','LAST_UPDATE_DATE','生成资产行金额','已转资金额']]
#list_township = df['项目编号','项目名称']
#list_township = df[['项目编号','项目名称','任务编号']]


list_result=list_township[list_township['项目编号'].notnull()]
print(list_result)

#for township in list_township:
#    save = df.loc[df["镇区"] == township]
#    save.to_csv('G:/分发服营/'+'宽带运营中心_'+str(township) +'_2kh20180131.txt',index=False,sep=',')

import pymysql
import pandas as pd
import numpy as np
import re

conn = pymysql.connect(host = 'kaishustory-out-dev.mysql.rds.？',port= 3306,user='',password ='',db = 'wx-pagelog',charset='utf8' )
data = pd.read_sql("""
    select * from wx_pagelog_20180612
    """,con = conn)

keys = ["uid","userid","deviceid","platfrom","app_version","code","activity_id","cms_id","page_share","click_name","tz"]

for key in keys:
    pat1 = '.*?' + key + '":"(.*?)".*?'
    pat2 = '^\{.*?\}$'
    data[key] = data['param'].apply(lambda x : 'N_param' if x is None else (re.findall(pat1,x) if re.match(pat2,x) else 'N_key'))

data = data.applymap(lambda x : np.nan if isinstance(x, list) and len(x)==0 else ( x[0] if isinstance(x, list) else x))
data['count'] = 1 

def func(a):
	writer = pd.ExcelWriter('打点验证pe.xlsx')
	for i in a:
		result = pd.pivot_table(data,index = ['page','event',i],columns =['project'],values =['count'],aggfunc = [np.sum])
		result.to_excel(writer,sheet_name = i+'_'+'project')
	writer.save()

a = list(data.columns)

remove_list = ['id','project','page','event','eventtime','createtime','count']
for rm_item in remove_list:
    a.remove(rm_item)
func(a)
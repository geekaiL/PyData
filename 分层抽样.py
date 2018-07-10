import pandas as pd
d = pd.read_excel(open('C:/Users/Edianzu/Desktop/用户抽样总体.xlsx','rb'))
d1 = pd.read_excel(open('C:/Users/Edianzu/Desktop/用户抽样总体 (1).xlsx','rb'))
d2 = pd.read_excel(open('C:/Users/Edianzu/Desktop/用户抽样总体 (2).xlsx','rb'))
d3 = pd.read_excel(open('C:/Users/Edianzu/Desktop/用户抽样总体 (3).xlsx','rb'))
frames = [d,d1,d2,d3]
data = pd.concat(frames)
sample = data.groupby('用户分类').apply(lambda x: x.sample(frac=(100000/len(data))))
sample.to_excel('C:/Users/Edianzu/Desktop/抽样数据1.xlsx')

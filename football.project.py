from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('demo').getOrCreate()
uefa=spark.read.option(header=True,inferSchema=True).csv('hdfc://localhost:900/sparkproject')
uefa.show()



# ---to print column names--
print('column name are:')

# ---column names--
for i in uefa.columns:
    print(i)

# --no.of.columns--
print('no.of.columns are:',len(uefa.columns))
#
# # -----no.of.rows----
print('no.of columns are:',uefa.count())
#
# analsis==1 draw a grp of away team and home team goal scoring in each year of quaterfinal and final(plot it as 2 graph)

from pyspark.sql import functions as f
newuefa=uefa.withColumn("date",f.from_unixtime(f.unix_timestamp(uefa.date),"yyyy-MM-dd"))
newuefa.show()
newuefa.printSchema()

from pyspark.sql.functions import udf
from pyspark.sql.functions import split



def yeargenerator(x):
    if x!="":
        li=x.split('-')
        return li[0]

myfn=udf(yeargenerator)
out=newuefa.withColumn('year',myfn(newuefa['date']))
out.show()

flt_out=out.filter((out['round']=='round:quarterfinals')|(out['round']=='round:semifinals')|(out['round']=='round:final'))
new=flt_out.select('homescore','awayscore','round','year')
new.show(n=50)

def myremove(value):
    return value[0]
newfn=udf(myremove)
one=new.withColumn('home_score',newfn(new['homescore']))
one.show()
result=one.withColumn('away_score',newfn(one['awayscore']))
result.show(n=50)
final=result.drop('homescore','awayscore')
final.show(n=50)
final.printSchema()

# --------convert string to integers for home_score and aways_score:-----------

from pyspark.sql.types import IntegerType

newdf=final.withColumn('hmscore',final['home_score'].cast(IntegerType()))
finaldf=newdf.withColumn('awscore',newdf['away_score'].cast(IntegerType()))
finaldf=finaldf.drop('home_score','away_score')
finaldf.show()
finaldf.printSchema()
#
# ------grping year wise:------

grp=finaldf.groupby('year').agg(f.sum(finaldf['hmscore']).alias('totalhomegoals'),f.sum(finaldf['awscore']).alias('totalawaygoals'))
grp=finaldf.groupby('year').agg(f.sum('hmscore').alias('totalhomegoals'),f.sum('awscore').alias('totalawaygoals'))
grp.show()
grp=grp.orderBy('year')
# #
# ---graphical representation------

import pandas as pd
df=grp.toPandas()

import matplotlib.pyplot as plt

plt.plot(df['year'],df['totalhomegoals'])
plt.plot(df['year'],df['totalawaygoals'])
plt.show()





# # -----analysis 2------
#
# ---------team that most appeared in quarterfinal and final-------

uefa=spark.read.options(header=True,inferSchema=True)('hdfs://localhost:9000/uefachampionsleague/football.csv')
uefa.show()

newuefa = uefa.filter((uefa['round'] == 'round : quarterfinals') | (uefa['round'] == 'round : semifinals') | (
            uefa['round'] == 'round : final'))
newuefa.show()
#
qf = uefa.filter(uefa['round'] == 'round : quarterfinals')
sf = uefa.filter(uefa['round'] == 'round : semifinals')
fi = uefa.filter(uefa['round'] == 'round : final')

qf.show()
sf.show()
fi.show()

qf = qf.select('_c0', 'homeTeam', 'round', 'date')
sf = sf.select('_c0', 'homeTeam', 'round', 'date')
fi = fi.select('_c0', 'homeTeam', 'round', 'date')

qf.show()
sf.show()
fi.show()

import pyspark.sql.functions as f

li = [qf, sf, fi]
for i in li:
    out1 = i.groupBy('homeTeam').agg(f.count('_c0').alias('no_of_participation'))
    out2 = out1.orderBy('no_of_participation', ascending=False)
    out2.show()
    maxvalue = out2.select(f.max(out2.no_of_participation))
    print(maxvalue.collect()[0])
    print('..................................................')






# -*- coding: utf-8 -*

"""
慢病预测模型的搭建
"""

import jieba
from pyspark.ml.feature import Word2Vec, Word2VecModel
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml import Pipeline, PipelineModel
import time


def cut_word(val):
    # 对数据进行分词
    if val is None:
        return ""
    val = cut_special_word(val)
    stop_list = get_stop_list()
    word_list = list()
    word = jieba.cut(val)
    for w in word:
        if w not in stop_list:
            word_list.append(w)
    return word_list


def cut_special_word(val):
    # 清洗不需要的词汇和标点
    re_list = ['适应症', '功能主治', u'\xa0']
    for re in re_list:
        val = val.replace(re, "").strip()
    return val


def get_stop_list():
    # 获取停用词汇
    with open('stop_word', 'r+', encoding='UTF-8') as f:
        word = f.read().splitlines()
    return word


def union_col(*kw):
    func = ""
    for k in kw:
        if k is not None:
            func = func + k
    return func


"""
读取CSV文件信息
"""
# 创建spark配置
# conf = SparkConf().setMaster('local').setAppName('test')
# 创建spark连接
# sc = SparkContext(conf=conf)
# 创建sparkSession
spark = SparkSession.builder.appName("chronic").master("local[*]").enableHiveSupport().getOrCreate()
# 使用spark读取csv文件
manbing = spark.read.csv("manbin.csv", header=True, mode="DROPMALFORMED")

"""
对所有主治功能的列进行合并,
把所有String类型的字段合成
一个总String字段
"""
# 使用udf来注册方法,并且指定输入和输出类型
toUnionUDF = udf(union_col, StringType())
# 指定需要合并的列并使用withColumn方法和udf自定义定义方法来进行转换，并生成一个新的列
manbing = manbing.withColumn('d_func', toUnionUDF(
              manbing.d_func_1, manbing.d_func_2, manbing.d_func_3,
              manbing.d_func_4, manbing.d_func_5, manbing.d_func_6,
              manbing.d_func_7, manbing.d_func_8, manbing.d_func_9,
              manbing.d_func_10, manbing.d_func_11, manbing.d_func_12,
              manbing.d_func_13, manbing.d_func_14, manbing.d_func_15,
              manbing.d_func_16))


"""
对所有主治功能总字段进行分词
使用jieba分词的方法进行
"""
start=time.time()
# 使用udf来注册方法,并且指定输入和输出类型
toDateUDF = udf(cut_word, ArrayType(StringType()))
manbing = manbing.withColumn('d_func', toDateUDF(manbing.d_func))
end=time.time()
print("分词用时：{}".format(end-start))

"""
先计算出正负例的比例
对数据进行分层抽样
"""

# 正例的比例
manbing_Positive = manbing.where(manbing.drug_1 == '1').count()
# 负例的比例
manbing_Nagtive = manbing.where(manbing.drug_1 == '0').count()

# 根据给定的比例进行随机抽样（负例23%和正例77%）并返回一个新的DataFrame（？？？）
manbing = manbing.sampleBy("drug_1", fractions={'0': 20000/manbing_Nagtive, '1': 20000/manbing_Positive}, seed=0)
# 从源数据中指定标签列
manbing = manbing.select("erp_code", "d_func", col("drug_1").cast("int").alias('label'))



"""
创建训练集和测试集
并且保存训练集和测试集
"""

start=time.time()
# 随机按比例拆分训练集70%和测试集30%
train_set, test_set = manbing.randomSplit([0.7, 0.3])
# 保存训练集和测试集
train_set.cache()
test_set.cache()

end=time.time()
print("数据拆分用时：{}".format(end-start))


"""
对主治功能总字段进行词转向量
"""

start = time.time()
# 创建word2Vec对象并指定配置信息（维度信息，输入列，输出列）
# word2Vec = Word2Vec(vectorSize=300, minCount=0, inputCol="d_func", outputCol="d_func_result")
# 使用word2Vec对象对指定列进行转化
# hypertension_model = word2Vec.fit(manbing)

# 持久化模型(费时操作只执行一次)
# hypertension_model.save("./WV_model/")

model = Word2VecModel.load("./WV_model/")

# 对训练集的数据进行词转向量的转化
manbing = model.transform(train_set)    
end=time.time()
print("词转向量用时：{}".format(end-start))


"""
训练数据构建模型
"""

start=time.time()
# 从源数据中指定特征列
assembler = VectorAssembler(inputCols=["d_func_result"], outputCol="features")
# assembler对象是一个transformer,将多列数据转化为单列的向量列(决策树可以识别的类型)
# train_set2 = assembler.transform(manbing)

# 构建决策树，配置标签列和特征列
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# 构建一个评估器(用于二分类问题的结果评估)
# 配置参数包括（评估的标签）、评估的单位AUC和ACC（默认AUC）、（？？？）
evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',
                                          labelCol='label',
                                          metricName='areaUnderROC')

# 构建参数网格旨在减少工作量及需要设置超参数（？？？）
# 参数包括决策树纯度参数,并设置基尼指数和熵、决策树的最大深度、决策树的最大划分数
paramGrid = ParamGridBuilder().addGrid(dt.impurity, ['gini', 'entropy'])\
    .addGrid(dt.maxDepth, [5, 10, 15]).addGrid(dt.maxBins, [10,])\
    .build()

# 构建交叉验证器
# 参数包括:决策树,评估器,评估器参数.
cv = CrossValidator(estimator=dt, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3)  #（？？？）

# 明确机器学习工作流并开始工作
cv_pipeline = Pipeline(stages=[assembler, cv])  # （？？？）

# 开始根据给定的资源进行训练
print("training.....")
cv_pipelineModel = cv_pipeline.fit(manbing)
# 在pipeline中第0个步骤中根据选择的模型输出最优模型并且写入指定文件夹（？？？）
bestModel = cv_pipelineModel.stages[1].bestModel
# bestModel.write().overwrite().save('./hypertension_model/')
end=time.time()
print("训练量用时：{}".format(end-start))

"""
验证模型
"""

start=time.time()
# 再次对测试集的数据进行词转向量的转化
test_set = model.transform(test_set)
# 再次将多列数据转化为单列的向量列(决策树可以识别的类型)
# test_set = assembler.transform(test_set)
# 再次使用cv_pipelineModel进行验证,把在pipeline中的所有transform都执行一遍（？？？）
bestDt = DecisionTreeClassifier(labelCol="label", featuresCol="features", impurity="entropy", maxDepth=bestModel.depth, maxBins=32)
dt_pipeline = Pipeline(stages=[assembler, bestDt])
dt_model = dt_pipeline.fit(manbing)
dt_model.write().overwrite().save("./models/hypertension_dtmodel/")
sameDTModel = PipelineModel.load("./models/hypertension_dtmodel/")

predictions = sameDTModel.transform(test_set)
# 使用评估器对预测结果进行评估得到auc
auc = evaluator.evaluate(predictions)
print("auc="+str(auc))
acc = predictions.filter(predictions['label'] == predictions['prediction']).count() / float(predictions.count())
print("acc="+str(acc))
end = time.time()
print("预测用时：{}".format(end-start))


"""
auc=0.9382650386182554
acc=0.8880381987321972

"""


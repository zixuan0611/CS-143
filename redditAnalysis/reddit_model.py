from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import cleantext
import pyspark.sql.functions as furuya
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
from pyspark.sql.functions import column, split


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    # task 1
    try:
        submissions = context.read.parquet("submissions.parquet")
        comments = context.read.parquet("comments.parquet")
        labeled_data = context.read.parquet("labeled_data.parquet")
    except:
        sqlContext = SQLContext(context)
        submissions = sqlContext.read.json("submissions.json.bz2")
        comments = sqlContext.read.json("comments-minimal.json.bz2")
        labeled_data = sqlContext.read.format('csv').options(header='true', inferSchema='true').load("labeled_data.csv")
        submissions.write.parquet("submissions.parquet")
        comments.write.parquet("comments.parquet")
        labeled_data.write.parquet("labeled_data.parquet")

    # task 2
    labeled_comments = comments.join(labeled_data, comments.id == labeled_data.Input_id).select(['Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body'])

    # task 4 & 5
    def cleandata(data):
        _, uni, bi, tri = cleantext.sanitize(data)
        new_gram = uni.split() + bi.split() + tri.split()
        return new_gram

    udf_data = udf(cleandata, ArrayType(StringType()))
    labeled_comments = labeled_comments.withColumn("n_gram", udf_data(labeled_comments.body).alias("n_gram"))

    # task 6A
    cv = CountVectorizer(inputCol="n_gram", outputCol="features", minDF=10.0, binary=True)
    model = cv.fit(labeled_comments)
    makiyo = model.transform(labeled_comments)
    model.save('project2/our_saved_model.model')

    # task 6B
    udf_pos = udf(lambda r : 1 if r == 1 else 0, IntegerType())
    udf_neg = udf(lambda r : 1 if r == -1 else 0, IntegerType())
    pos_label = makiyo.select("*", udf_pos('labeldjt').alias("label"))
    neg_label = makiyo.select("*", udf_neg('labeldjt').alias("label"))
    pos_label = pos_label.withColumn("label", (column("label")).alias("label"))
    neg_label = neg_label.withColumn("label", (column("label")).alias("label"))

    # task 7

    # Bunch of imports (may need more)
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.2)
    neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.25)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=5)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = pos_label.randomSplit([0.5, 0.5])
    negTrain, negTest = neg_label.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)

    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("project2/pos.model")
    negModel.save("project2/neg.model")

    # task 8
    comments = comments.select('body', 'created_utc', column('author_flair_text').alias('state'), column('score').alias('com_score'), udf(lambda x: x[3:])('link_id').alias('link_id'))
    comments = comments.filter(~comments.body.contains('/s')).filter(~comments.body.startswith('&gt;'))
    submissions = submissions.select('id', 'title', column('score').alias('sub_score'))
    combined = comments.join(submissions, comments.link_id == submissions.id).drop('id')

    # task 9
    combined = combined.select('link_id', 'title', 'body', 'created_utc', 'state', 'com_score', 'sub_score')
    combined = combined.withColumn("n_gram", udf_data(combined.body).alias("n_gram"))
    combined = model.transform(combined)
    posResult = posModel.transform(combined)
    def udf_function1(x):
        if x[1] <= 0.2:
            return 0
        else:
            return 1
    def udf_function2(x):
        if x[1] <= 0.25:
            return 0
        else:
            return 1
    udf_1 = udf(udf_function1, IntegerType())
    udf_2 = udf(udf_function2, IntegerType())
    posResult = posResult.select('link_id', 'title', 'created_utc', 'com_score', 'state', 'sub_score', 'features', column('rawPrediction').alias('pos_rawPredict'), column('prediction').alias('pos_predict'), udf_1(posResult.probability).alias("pos_prob"))
    #posResult = posResult.withColumn("pos_rawPredict", column('rawPrediction').alias('pos_rawPredict'))
    #posResult = posResult.withColumn("pos_predict", column('prediction').alias('pos_predict'))

    #posResult = posResult.withColumn("pos_prob", udf_1(posResult.probability).alias("pos_prob"))

    negResult = negModel.transform(posResult)

    negResult = negResult.select('pos_prob', 'pos_predict', 'pos_rawPredict', 'link_id', 'title', 'created_utc', 'com_score', 'state', 'sub_score', column('rawPrediction').alias('neg_rawPredict'), column('prediction').alias('neg_predict'), udf_2(negResult.probability).alias("neg_prob"))
    #negResult = negResult.withColumn("neg_rawPredict", column('rawPrediction').alias('neg_rawPredict'))
    #negResult = negResult.withColumn("neg_predict", column('prediction').alias('neg_predict'))

    #negResult = negResult.withColumn("neg_prob", udf_2(negResult.probability).alias("neg_prob"))

    # task 10
    # 1: across all submissions
    rei = negResult.groupBy('link_id','title').agg((furuya.sum('neg_prob')/furuya.count('neg_prob')).alias('neg'), (furuya.sum('pos_prob')/furuya.count('pos_prob')).alias('pos'))

    rei = rei.select('link_id','title','pos','neg')

    rei.orderBy("pos", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("top_pos_posts.csv")
    rei.orderBy("neg", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("top_neg_posts.csv")

    # 2: across all days
    ichi = negResult.groupBy(furuya.from_unixtime('created_utc', 'yyyy-MM-dd').alias('date')).agg((furuya.sum('pos_prob')/furuya.count('pos_prob')).alias('pos'),(furuya.sum('neg_prob')/furuya.count('neg_prob')).alias('neg'))

    ichi = ichi.select('date','pos','neg')
    ichi.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("days.csv")

    # 3: across all states
    states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
    san = negResult.filter(column('state').isin(states)).groupBy(column('state').alias('state')).agg((furuya.sum('pos_prob')/furuya.count('pos_prob')).alis('pos'),(furuya.sum('neg_prob')/furuya.count('neg_prob')).alias('neg'))

    san = san.select('state','pos','neg')
    san.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("states.csv")

    # 4.comments_score

    com_score = negResult.groupBy('com_score').agg((furuya.sum('pos_prob')/furuya.count('pos_prob')).alis('pos'), (furuya.sum('neg_prob')/furuya.count('neg_prob')).alias('neg'))

    com_score = com_score.select('com_score', 'pos', 'neg')

    # 4.submissions_score
    sub_score = negResult.groupBy('sub_score').agg((furuya.sum('pos_prob')/furuya.count('pos_prob')).alis('pos'), (furuya.sum('neg_prob')/furuya.count('neg_prob')).alias('neg'))

    sub_score = sub_score.select('sub_score', 'pos', 'neg')



    sub_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("sub_score.csv")
    com_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("com_score.csv")




if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

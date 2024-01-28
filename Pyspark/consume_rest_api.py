import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row

def executeRestApi(verb, url, body, page):
    headers = {
        'Content-Type': "application/json",
        'User-Agent': "apache spark 3.x"
    }
    res=None
    try:
        if verb == "get":
            res = requests.get(f"{url}/{page}", data=body, headers=headers)
        else:
            res = requests.post(f"{url}/{page}", data=body, headers=headers)   
    except Exception as e:
        return e
    if res != None and res.status_code == 200:
        return json.loads(res.text)
    return None

schema = StructType([
  StructField("maxRecords", IntegerType(), True),
  StructField("results", ArrayType(
    StructType([
      StructField("Make_ID", IntegerType()),
      StructField("Make_Name", StringType())
    ])
  ))
])

udf_executeRestApi = udf(executeRestApi, schema)

body = json.dumps({})
page = 1
ResApiRequestRow=Row("verb", "url", "body", "page")
request_df = spark.createDataFrame([
    ResApiRequestRow("get", "https://tools.rulenumberfour.co.uk/api/vehicles", body, page)
])

result_df = request_df.withColumn("result", udf_executeRestApi(
                                                                F.col("verb"),
                                                                F.col("url"),
                                                                F.col("body"),
                                                                F.col("page")
                                                            )
                                  )
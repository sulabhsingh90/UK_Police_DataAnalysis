# Analysis on UK Police Data using Apache spark (scala)

> Dataset used in this exaple is from [**UK POLICE DEPARTMENT**][1]
  It is available under [**Open Government Licence v3.0**](  https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

Apache spark is getting popularity in Data Engineers as well as Data Science because of its variety of components:

* Spark SQL/Dataframes/Datasets
* Spark MLlib
* GraphX
* Spark Streaming

We will use Spark SQL/Dataframe for this example. Got to [**UK POLICE DEPARTMENT**][1] website and select date range from March 2018 to April 2018,tick **All forces** checkbox and hit **Generate file** button. It will lead you to download page.
___
## Structure of our CSV files

Crime ID|Month|Reported by|Falls within|Longitude|Latitude|Location|LSOA code|LSOA name|Crime type|Last outcome category|Context
---|---|---|---|---|---|---|---|---|---|---|---
5ead780133e271220de3fe6215a2ef5c0cdaf09459efd1d599a5390f3aeb5c6a|2018-03|Avon and Somerset Constabulary|Avon and Somerset Constabulary|-2.511571|51.414895|On or near Orchard Close|E01014399|Bath and North East Somerset 001A|Vehicle crime|Under investigation|
5d20e6f6fb0befd7ed627abe332d3bc0dda5312249b42ccca71968a5440ba299|2018-03|Avon and Somerset Constabulary|Avon and Somerset Constabulary|-2.511571|51.414895|On or near Orchard Close|E01014399|Bath and North East Somerset 001A|Vehicle crime|Under investigation|
45e332d048fe54325cb5e8601976b49dd3225fbcd0cda8fd7f507b7fdf307fec|2018-03|Avon and Somerset Constabulary|Avon and Somerset Constabulary|-2.511571|51.414895|On or near Orchard Close|E01014399|Bath and North East Somerset 001A|Violence and sexual offences|Under investigation|

## Let us define our problem statement for this example

1. List all Crime Type reported by UK Police.
2. Find out frequency of each crimes (Number of crimes committed)
3. Find out percentage of cases where accused are not guilty in outcome
___

## Lets jump into some action

Extract downloaded folder at any handy location on your machine. I am assuming you already have a working SCALA Eclipse IDE, check out this example in your workspace.

```linux
	git clone https://github.com/sulabh92/UK_Police_DataAnalysis.git
```

Now you have a Scala project in your eclipse. Go to **_DataAnalysis.scala_**

First we have to initiate our Spark Session

```scala
val spark=SparkSession.builder().appName("DataAnalysis").master("local[*]").getOrCreate()
```
:+1: Our SparkSession is initated with **spark**.

Now we will read our data with the help of dataframe reader.

```scala
val ukPoliceDF=spark.read.format("csv").option("header", "true").option("mode", "FAILFAST")
				.option("inferSchema", "true")
				.option("path", "/UKPolice_data/*")
				.load()
```
> Avoid inferSchema in production enviorment as it takes time and you can eliminate other issues too. For e.g: Our Dataset have spaces in between column name that can cause issue during development.
> It is recommended to create manual schema on production enviorment.
* List distinct Crime type from our Dataframe

```scala
ukPoliceDF.select("Last outcome category").distinct().collect().foreach(println)
```
 Hopefully, you will get in your console
 ```
 [Offender ordered to pay compensation]
[Offender given suspended prison sentence]
[Defendant sent to Crown Court]
[Offender given penalty notice]
[Suspect charged as part of another case]
[null]
[Local resolution]
[Offender given a caution]
[Offender given conditional discharge]
[Investigation complete; no suspect identified]
[Offender given absolute discharge]
[Under investigation]
[Awaiting court outcome]
[Defendant found not guilty]
[Offender sent to prison]
[Further investigation is not in the public interest]
[Action to be taken by another organisation]
[Offender given community sentence]
[Offender given a drugs possession warning]
[Formal action is not in the public interest]
[Offender fined]
[Court case unable to proceed]
[Offender otherwise dealt with]
[Offender deprived of property]
[Unable to prosecute suspect]
 ```
* Find out frequency of each crimes (Number of crimes committed)

```scala
val topCrime=ukPoliceDF.groupBy(col("crime type"))
				.count().withColumnRenamed("count", "total")
				.orderBy(desc("total"))

				topCrime.collect().foreach(println)
```
> Have you noticed that we have mentioned Crime Type column with **col** in groupBy, but in **withColumnRenamed** method we are simply passing column name as string. This is because Spark will compile your expressions or functions or sql logics down to an underlying plan before actually executing , so every statement will run eactly the same beneath the skin.

OUTPUT:
```
[Violence and sexual offences,283132]
[Anti-social behaviour,235834]
[Criminal damage and arson,96982]
[Other theft,89430]
[Vehicle crime,76677]
[Public order,69989]
[Burglary,68621]
[Shoplifting,66085]
[Drugs,23755]
[Other crime,16914]
[Theft from the person,16101]
[Robbery,12966]
[Bicycle theft,12235]
[Possession of weapons,7680]
```

* Find out percentage of cases where accused are not guilty in outcome

```scala
val notGuilty=ukPoliceDF.groupBy("crime type").agg(count("*").alias("total"),
						sum(when(col("Last outcome category")==="Defendant found not guilty",lit(1)).otherwise(lit(0))
								).alias("notguilty")).withColumn("notguiltypercent", bround(col("notguilty")/col("total")*lit(100),2)).orderBy(desc("notguilty"),desc("total"))

				notGuilty.collect().foreach(println)
```

> We can use **agg** functions to do some complex aggregation on RelationalGroupedDataset. "Defendant found not guilty" is a value present in dataset, **lit()** function is used to define sparkType literals.
>**asc,desc,count,expr,sum,when,col,lit,bround** are functions imported from **org.apache.spark.sql.functions**

OUTPUT:

```
[Violence and sexual offences,283132,160,0.06]
[Public order,69989,32,0.05]
[Shoplifting,66085,21,0.03]
[Criminal damage and arson,96982,17,0.02]
[Burglary,68621,17,0.02]
[Other crime,16914,16,0.09]
[Possession of weapons,7680,9,0.12]
[Drugs,23755,8,0.03]
[Other theft,89430,5,0.01]
[Vehicle crime,76677,5,0.01]
[Robbery,12966,4,0.03]
[Theft from the person,16101,3,0.02]
[Anti-social behaviour,235834,0,0.0]
[Bicycle theft,12235,0,0.0]
```

> ### Warning:
> Collect is very expensive operation that can crash your driver program when dealing with large dataset. Its also not recommended as it will process data one by one basis instead of running in parllel

---
#### I will try to cover more Spark SQL/Dataframe functions in future commits. 
> Happy Learning

[1]:https://data.police.uk/data/


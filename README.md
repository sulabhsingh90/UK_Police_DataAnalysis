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

[1]:https://data.police.uk/data/


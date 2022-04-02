
# Chapter 2

Chapter 2 of the book contains a first Spark example. We will explain how to run it on your machine. 

## The task
The task of the program is to analyze a data describing the consumption of M&Ms in different colors on events in different states. The file `mnm_dataset.csv`contains 100,000 lines of data points. Each data points describes the consumption of M&Ms in a certain color on an event in a certain state. Here is an example of the data points in the file:
```
State,Color,Count
TX,Red,20
NV,Blue,66
CO,Blue,79
```
The first line of the file contains the schema and describes the three different columns. The file `mnm_dataset.csv` and the Python file `mnmcount.py`can be downloaded from the [github repo](https://github.com/databricks/LearningSparkV2/tree/master/chapter2/py/src) of the book. 

The task of the program is to aggregate all data points by state and color and sort the output by the number of M&Ms consumed. In a second output, the program filters the results for the state California (CA).

## Running the M&M Example
Download the Python file `mnmcount.py` to a local directory on your machine which is running Spark.

Download the data file`mnm_dataset.csv` into the subdirectory `data`

Run the command: 
```
$SPARK_HOME/bin/spark-submit mnmcount.py data/mnm_dataset.csv
```
To switch off all the info messages coming from the console, you can set `log4j.rootCategory=WARN` in the `log4j.properties` file in the directory `$SPARK_HOME/conf` on your machine. 

This is the output of the program:
```
+-----+------+-----+
|State|Color |Total|
+-----+------+-----+
|CA   |Yellow|1807 |
|WA   |Green |1779 |
|OR   |Orange|1743 |
|TX   |Green |1737 |
|TX   |Red   |1725 |
|CA   |Green |1723 |
|CO   |Yellow|1721 |
|CA   |Brown |1718 |
|CO   |Green |1713 |
|NV   |Orange|1712 |
|TX   |Yellow|1703 |
|NV   |Green |1698 |
|AZ   |Brown |1698 |
|WY   |Green |1695 |
|CO   |Blue  |1695 |
|NM   |Red   |1690 |
|AZ   |Orange|1689 |
|NM   |Yellow|1688 |
|NM   |Brown |1687 |
|UT   |Orange|1684 |
|NM   |Green |1682 |
|UT   |Red   |1680 |
|AZ   |Green |1676 |
|NV   |Yellow|1675 |
|NV   |Blue  |1673 |
|WA   |Red   |1671 |
|WY   |Red   |1670 |
|WA   |Brown |1669 |
|NM   |Orange|1665 |
|WY   |Blue  |1664 |
|WA   |Yellow|1663 |
|WA   |Orange|1658 |
|CA   |Orange|1657 |
|NV   |Brown |1657 |
|CA   |Red   |1656 |
|CO   |Brown |1656 |
|UT   |Blue  |1655 |
|AZ   |Yellow|1654 |
|TX   |Orange|1652 |
|AZ   |Red   |1648 |
|OR   |Blue  |1646 |
|UT   |Yellow|1645 |
|OR   |Red   |1645 |
|CO   |Orange|1642 |
|TX   |Brown |1641 |
|NM   |Blue  |1638 |
|AZ   |Blue  |1636 |
|OR   |Green |1634 |
|UT   |Brown |1631 |
|WY   |Yellow|1626 |
|WA   |Blue  |1625 |
|CO   |Red   |1624 |
|OR   |Brown |1621 |
|TX   |Blue  |1614 |
|OR   |Yellow|1614 |
|NV   |Red   |1610 |
|CA   |Blue  |1603 |
|WY   |Orange|1595 |
|UT   |Green |1591 |
|WY   |Brown |1532 |
+-----+------+-----+

Total Rows = 60
+-----+------+-----+
|State|Color |Total|
+-----+------+-----+
|CA   |Yellow|1807 |
|CA   |Green |1723 |
|CA   |Brown |1718 |
|CA   |Orange|1657 |
|CA   |Red   |1656 |
|CA   |Blue  |1603 |
+-----+------+-----+

```


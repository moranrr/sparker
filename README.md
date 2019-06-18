[toc]

语法：

**load** | **save** | **execute** | **set** | **check**  xxx **with** prams1="value1 xxx" & param2 = value1 **as** temptableName;


每个task必须以";"结尾。

# Future
1. 可以设置失败策略，某个task失败后可以选择忽略。
2. 可以为为所有task或者某个task设置重试次数。
3. 可以检查输出表的质量。



# Source

**使用load加载数据**。

## json文件
读取json文件，并且映射成表。
例：
```
load json."file:///E:\test"  with isPersist=true   as jsontable;
```
读取本地文件``"E:\test"``,并映射成表jsontable。

可选参数：

name | desc | default
--- | --- | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区） | null

## parquet文件

读取parquet文件文件，并且映射成表。
例：
```
load parquet."file:///E:\test"  with isPersist=true   as parquettable;
```
读取本地文件``"E:\test"``,并映射成表parquettable。

可选参数：

name | desc | default
--- | --- | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区 | null


## text文件、

读取text文件文件，并且映射成表。
例：
```
load text."file:///E:\test"  with isPersist=true   as texttable;
```
读取本地文件``"E:\test"``,并映射成表texttable，表只有一个字段``value``,值是文件的行。

可选参数：

name | desc | default
--- | --- | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区 | null

## csv文件

读取csv文件文件，并且映射成表。
例：
```
load csv."/path/csvfilepath" with header="true" & delimiter="," & encoding="utf-8" as temp_table;
```
读取本地文件``"/path/csvfilepath"``,并映射成表 temp_table。

可选参数：

name | desc | default
--- | --- | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区 | null
delimiter | 指定分隔符 | ","
encoding |  | 默认UTF-8
escape | 自定义转义字符| 默认“\\\”
header | 是否有头信息 | 必填
dateFormat | yyyy-MM-dd
timestampFormat | yyyy-MM-dd'T'HH:mm:ss.SSSXXX
multiLine | 
maxColumns | 
maxCharsPerColumn | 
escapeQuotes | 
quoteAll | 
ignoreLeadingWhiteSpace | 
ignoreTrailingWhiteSpace | 

## jdbc

使用jdbc连接读取数据。

```
load jdbc."( select a,c,b from table1 ) as t" 
with driver="com.mysql.jdbc.Driver" 
& url="jdbc:mysql://10.168.65.21:3652/activity?autoReconnect=true&user=activity&password=activity" 
& user = "123" & password=1233   
as temp_table;
```

读取表``"table1``的``a,c,d``三个字段,并映射成表 temp_table。

参数：

name | 描述 | 说明
--- | ---  | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区 | null
url | URL连接 | 必须
driver | 驱动类全名 | 必须
user | 用户名 | 
password | 密码 |
partitionColumn | 分区字段，必须为数字 | null
lowerBound | 如果使用分区字段 分区字段的最小值 | null
upperBound | 如果使用分区字段 分区字段的最大值 | null
numPartitions | 多少个分区 ，每个分区的数据约等于(upperBound - lowerBound)/partitionColumn | 1

## excel

和csv类似，读取excel第一个shell页，并映射成表。不支持多shell页，和列行范围。

```
load excel."file:///E:\testexcel\test.xlsx" with useHeader=true
& schema="[A String,B String,C Int,D Int]"
&treatEmptyValuesAsNulls=true&inferSchema=false&
 as exceltable;
```

参数：

name | 描述 | 说明
--- | ---  | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区 | null
useHeader | 是否有头信息 | 必填
inferSchema | 是否自动推测列的类型 | false
maxRowsInMemory | 如果设置了这个值，就使用文件流读取，大文件的时候使用 | none
excerptSize | 如果inferSchema设置成了true，使用前多少行的数据推测列的类型 | 10
workbookPassword | 对于较旧的JVM，需要的JCE | None
schema | 自定义列的类型,例如：[A String,B String,C Int,D Int] | None

## hive

见execute

# execute

## sql

执行hivesql，从hive表中读取数据，或者保存数据到hive表。

```
execute sql."hdfs://gyuj/xxx.sql" as table1;

execute sql."select clos from hivetable" as table1;

execute sql."insert overwrite table xxx select xcx from ffff " as table1;
```

支持从文件中读取sql。sql可进行参数替换。
如：sql：
```
execute sql."insert overwrite table xxx partition(pday=:pday)....

```
``:pday``就是占位符，会被启动脚本中的参数``sparker.sql.params.pday``的值替换。

如果sql是以``file://``或者``hdfs://``开头，则会从文件中读取sql。
执行多条sql语句，多条语句每条sql最后一行以";"结尾。并且不会输出任何临时表。

参数：

name | 描述 | 说明
--- | ---  | ---
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数（如果需要重新分区） | null
filureStrategy | 失败策略 | 默认abort，退出。continue：发送文件然后继续往下执行

## jdbcsql

使用jdbc数据源执行一条sql，比如建表，或者删除数据等。

```
execute jdbcsql."truncate table xxxx" with  driver="com.mysql.jdbc.Driver" 
& url="jdbc:mysql://192.168.150.242:3306/activity?autoReconnect=true&user=activity&password=activity" 
& user = "123" & password=1233 & poolsize=1;
```

name | 描述 | 说明
--- | ---  | ---
url | URL连接 | 必须
driver | 驱动类全名 | 必须
user | 用户名 | 
password | 密码 |

## scala

执行scala脚本，用于数据的复杂处理，但是每次只能处理一行，如果要处理多行，可以使用自定义函数。

```
execute scala."jsontable" with script="file:///C:\Users\Desktop\sparker\scala\script.scala"  & ignoreOldColumns=false as newtable;
```

scala脚本使用参数rowmap:Map[String,Any]，表示一行的数据。返回值为类型Map[String,Any]。

name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送文件然后继续往下执行
isCache | 是否缓存，如果这个临时表，在后续的task中使用超过1次，cache效率更高 | false
numPartitions | 重新分区数 | null
script | scala脚本，如果以``file://``或者``hdfs://``开头，则会从文件中读取 | 必须
schame | 自定义的创建StructType的脚本，返回值是Option[StructType] | None
ignoreOldColumns | 是否忽略旧的行，如果true，则新的数据中只包括脚本中返回的数据，如果是false，和返回合并后的数据。 | false


# check 

语法：

```
check "count sql" > | < | = | >= | <= | !=  int_value Abort | WARN | ignore
```

例子：
```
load json."file:///E:\test"  with isPersist=true  as jsontable;
-- 如果行数小于100 退出
check "select count(1) from jsontable" < 100 Abort;
```

参数：

name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送文件然后继续往下执行。
mailSender | 发送方的邮件地址 | 必须
password | 发送方的邮件密码 | 必须
smtpHost |
toMails | 收件人地址，多个用逗号隔开 | 必须

以上参数可以设置为全局参数。

# set

设置全局参数：

```
set spark.es.nodes=10.86.210.23;
set spark.es.port=9200;
set spark.es.index.auto.create=true;
set spark.es.net.http.auth.user=username;
set spark.es.net.http.auth.pass=password; 

set mailSender="xxx@163.com";
set password="xxxxxx";
set smtpHost="outlook.163.com";
set toMails="wafys@163.com";
```

以``spark.``或者``hive.``开头的参数会传递给SparkConf,但是job描述文件中的spark参数优先级小于shell脚本的参数。

# sink

使用save语法保存数据。

语法：
```
SAVE [Overwrite|Append|Ignore|ErrorIfExist] inputTableName sinkType."target" [with params1=val1 & param2=val2];

```

## JSON/CSV/TEXT/PARQUET

例：
```
save table_name as json."jsonpath" with  fileNum = 10 & filureStrategy="continue";
```

参数：
name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送邮件然后继续往下执行。
fileNum | 落地的文件个数 | partiton_num
partitionBy | 分区字段
## JDBC 

不灵活。
```
save table_name as jdbc."db.table" with 
driver="com.mysql.jdbc.Driver" 
& url="jdbc:mysql://10.168.65.21:3652/activity?autoReconnect=true&user=activity&password=activity" 
& user = "123" & password=1233  
```

## Hive

```
save overwrite table_name as hive."db.table" with  fileNum = 10 & filureStrategy="continue";
```

参数：
name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送邮件然后继续往下执行。
fileNum | 落地的文件个数 | partiton_num
partitionBy | 分区字段


## Excel

```
load json."file:///E:\test" as jsontable;

save append jsontable as excel."file:///E:\testexcel\test.xlsx" with useHeader=false ;
```

注意append会新增一个sheet，不是在原有sheet上添加。

参数：
name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送邮件然后继续往下执行。
useHeader | 是否包含头信息 | 必填

## elasticserch

```
set spark.es.nodes=90.16.10.23;
set spark.es.port=9200;
set spark.es.index.auto.create=true;
set spark.es.net.http.auth.user=username;
set spark.es.net.http.auth.pass=password; 

set mailSender="wafys@163.com";
set password="email password";
set smtpHost="outlook.163.com";
set toMails="wafys@163.com";

load json."file:///E:\test"  with isPersist=true   as jsontable;

execute sql."hdfs://gyuj" with filureStrategy="continue";

execute check."select count(1) from jsontable" < 100 warn;

save jsontable as es."index/type" with  retry=3;
```

es的相关参数，必须设置为全局参数，或者在提交的shell中指定。

## sql

```
load json."file:///E:\test"  with isPersist=true   as jsontable;

execute sql."select A aaa,B bbb,C ccc, D ddd from jsontable" as temptable;

execute jdbcsql."truncate table qm.test1" with driver="ru.yandex.clickhouse.ClickHouseDriver" 
&url="jdbc:clickhouse://192.168.381.4:8123/qm?connect_timeout=60000&http_connection_timeout=60000" & poolsize =1 &;

save temptable as sql."insert into test1(aaa,bbb,ccc,ddd) values(?,?,?,?)" with driver="ru.yandex.clickhouse.ClickHouseDriver" 
&url="jdbc:clickhouse://192.168.381.4:8123/qm?connect_timeout=60000&http_connection_timeout=60000"
poolsize =1 & retry=2;
```

执行insert 语句向目标jdbc表写入数据。

参数：
参数：
name | 描述 | 说明
--- | ---  | ---
filureStrategy | 失败策略 | 默认abort，退出。continue：发送邮件然后继续往下执行。
retry | 重试次数 | 0
poolsize | 连接池大小 | 1
url | URL连接 | 必须
driver | 驱动类全名 | 必须
user | 用户名 | 
password | 密码 |
batchsize | 批量插入的个数 | 50000


向多个数据库中的表写数据，url用"|"隔开，此时会将数据平均写入指定的数据库，主要用于clickhouse的分布式表。

## console

打印数据，用于调试

```
-- 打印100条数据
save jsontable as console."100";
```

# 其他

1. 重试次数可以设置全局参数，但是不建议
```
set retry=3;
```
2. 设置错误策略为``filureStrategy=continue``时，需要设置email的全局参数，否则，不会发邮件。

3. 注释符号为``"--"`` 

4. 如果在某个sql中包含``paramsFile``参数，并且是以以``file://``或者``hdfs://``开头,则从指定文件中读取参数并合并。文件中参数格式为``properties``


# 提交脚本示例：

```
spark-submit \
--class com.datatec.sparker.core.StartApp \
--master yarn \
--deploy-mode client \
--name sparktest \
--num-executors 5 \
--executor-memory 8G \
--executor-cores 2 \
--driver-memory 4G \
--queue root.default \
--conf spark.sql.shuffle.partitions=30 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \
--files hdfs://gyuj/user/hive/hive-site.xml,hdfs://gyuj/user/hbase/hbase-site.xml  \
/opt/sspro/sparker-0.0.1.jar  \
-sparker.name sparktest   \
-sparker.enableHiveSupport true \
-sparker.sql.params.yestoday  $yestoday \
-sparker.log.params true \
-sparker.job.file.path hdfs://gyuj/user/gddd/project/xxxx/${1}

```

参数说明:

参数名称 | 参数含义 | 是否必须
--- | --- | ---
sparker.name | 在yarn中显示的任务名称 | 是
sparker.job.file.path | 任务描述文件路径 | 是
sparker.master | 如果使用本地调试，必须设置成local，yarn不用设置 | 否
sparker.udf.clzznames | 自定义函数的全路径类名，多个用逗号分隔 | 否
sparker.params.xxx | 传参。如某任务描述参数with daily=starttime,则shell脚本中的参数名是-sparker.param.starttime=20190618，则在任务运行时，会用20190618替代starttime，即daily的值实际为20190618。 | 否
sparker.sql.params.xxx | sql中的参数替换。例如，sql中有占位符:pay，在shell中指定sparker.sql.params.pday=20190618，则在运行sql时，:pay会被替换成20190618 | 否
sparker.log.params | 是否打印参数，用于调试，默认为false | 否
sparker.enableHiveSupport | 是否开启hive支持，默认true。在没有hive的环境下运行，需要设置为false | 否













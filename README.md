# db-http-sql

```
Usage of ./http_db:
  -d	debug msg (default true)
  -dns string
    	ClickHouse   tcp://127.0.0.1:42722?debug=false&database=azom_db&write_timeout=5&compress=true&username=default&password=password 
    	sqlserver    sqlserver://kangshu:bzynj@127.0.0.1:1433/?database=weixin&encrypt=disable 
    	Mysql        root:root@tcp(127.0.0.1:3306)/test 
    	Oracle       oracle://H2:hysoft@127.0.0.1:3521/hyee (default "root:1b0ef5fde8798137@tcp(127.0.0.1:3306)/umami")
  -i int
    	SlidingWindow 100/30 s (default 1000)
  -p string
    	服务端口 (default "8004")
  -t string
    	mysql、sqlserver、oracle、clickhouse (default "mysql")
  -w string
    	允许访问的ip逗号隔开
```
     

![image](https://user-images.githubusercontent.com/29120060/217162439-9548ad0a-1861-4817-a9b9-254e353766c5.png)

    (MySQL,Oracle,sqlserver,clickhouse,elasticsearch,es)数据服务 —— 写个 SQL 即可发布成 API


    SELECT * FROM goods_0 WHERE `goods_name` like '%家庭%'


```
//oracle://H2:hydeesoft@127.0.0.1:3521/hydee 
//root:ef08ef776ce21a44@tcp(127.0.0.1:3306)/after
//sqlserver://kangshu:bzdmmynj@127.0.0.1:1433/?database=weixin&encrypt=disable
//tcp://127.0.0.1:42722?debug=false&database=azdb&write_timeout=5&compress=true&username=default&password=xieyuhua
//127.0.0.1:9200

[root@Web6 oraclesql]# ./db-http-sql 
2023/02/02 10:41:31 监听端口 http://127.0.0.1:8086
```



感谢开源

```
github.com/sirupsen/logrus
github.com/ClickHouse/clickhouse-go
github.com/denisenkom/go-mssqldb
github.com/go-sql-driver/mysql
github.com/wdrabbit/gorm-oracle
github.com/elastic/go-elasticsearch
github.com/farmerx/elasticsql
```

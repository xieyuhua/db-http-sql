package main

import (
	"encoding/json"
	"fmt"
	"log"
 	"time"
    // "os"
    "github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"database/sql"
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
    _ "github.com/go-sql-driver/mysql"
	oracle "github.com/wdrabbit/gorm-oracle"
	"gorm.io/gorm"
	"context"
	"crypto/tls"
	"net"
	"github.com/elastic/go-elasticsearch"
	"github.com/farmerx/elasticsql"
	"github.com/spf13/cast"
	"gopkg.in/natefinch/lumberjack.v2"
)
var rows *sql.Rows
var logs = logrus.New()

//主函数
func main() {
    
    logs.SetFormatter(&logrus.JSONFormatter{})
    
	logger := &lumberjack.Logger{
		Filename:   "logrus.log",
		MaxSize:    50,  // 日志文件大小，单位是 MB
		MaxBackups: 3,    // 最大过期日志保留个数
		MaxAge:     30,   // 保留过期文件最大时间，单位 天
		Compress:   true, // 是否压缩日志，默认是不压缩。这里设置为true，压缩日志
	}

	logs.SetOutput(logger) // logrus 设置日志的输出方式
    
    
// 	logs.Out = os.Stdout
//     file, err := os.OpenFile("logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
//     if err == nil {
//         logs.Out = file
//     } else {
//         logs.Info("Failed to log to file, using default stderr")
    // }
    
    
	http.HandleFunc("/", querySql)
	link := "http://127.0.0.1:8785"
	log.Println("监听端口", link)
	listenErr := http.ListenAndServe(":8785", nil)
	if listenErr != nil {
		log.Fatal("ListenAndServe: ", listenErr)
	}
}

type JsonRes struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func querySql(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")
	defer func() {
		//捕获 panic
		if err := recover(); err != nil {
			log.Println("查询sql发生错误", err)
		}
	}()
	if r.URL.Path != "/" {
		w.WriteHeader(404)
		msg, _ := json.Marshal(&JsonRes{Code: 4000, Msg: r.URL.Path + " 404 NOT FOUND !"})
		w.Write(msg)
		return
	}

	r.ParseForm() // 解析参数
    // oracle,mysql,sqlsver
	d := r.PostFormValue("d")
	d = fmt.Sprintf("%s", d)
	log.Println("d:", d)
	
	driver := map[string]string{
		"mysql":  "mysql",
		"mssql":  "mssql",
		"sqlserver":"sqlserver",
		"oracle": "godror",
		"adodb":  "adodb",
		"clickhouse":"clickhouse",
		"es":"elasticsearch",
		"elasticsearch":"elasticsearch",
	}
	if _, ok := driver[d]; !ok {
		w.WriteHeader(404)
		msg, _ := json.Marshal(&JsonRes{Code: 4001, Msg: " 404 TYPE NOT FOUND !"})
		w.Write(msg)
		return
	}
	//oracle://H2:hydeesoft@127.0.0.1:3521/hydee 
	//root:ef08ef776ce21a44@tcp(127.0.0.1:3306)/after
	//sqlserver://kangshu:bzdmmynj@127.0.0.1:1433/?database=weixin&encrypt=disable
	//tcp://127.0.0.1:42722?debug=false&database=azmbk_com_db&write_timeout=5&compress=true&username=default&password=xieyuhua
	//127.0.0.1:9200
	s := r.PostFormValue("s")
	s = fmt.Sprintf("%s", s)
	log.Println("s:", s)
	
	if s=="" {
		w.WriteHeader(404)
		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 source NOT FOUND !"})
		w.Write(msg)
		return
	}
	
	
	//select * from a where id>100 limi 10
	sqls := r.PostFormValue("sql")
	sqls = fmt.Sprintf("%s", sqls)
	log.Println("sql:", sqls)
	
	
    logs.WithFields(logrus.Fields{
        "db": d,
        "source": s,
    }).Info(sqls)
    
    if d=="es"{
    	cfg := elasticsearch.Config{
    		Addresses: []string{
    			"http://"+s,
    		},
    		Transport: &http.Transport{
    			MaxIdleConnsPerHost:   10,
    			ResponseHeaderTimeout: time.Second,
    			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
    			TLSClientConfig: &tls.Config{
    				MaxVersion:         tls.VersionTLS11,
    				InsecureSkipVerify: true,
    			},
    		},
    	}
    	es, err := elasticsearch.NewClient(cfg)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error creating the client!"})
    		w.Write(msg)
    		return 
    	}
        esql := elasticsql.NewElasticSQL()
        table, dsl, err := esql.SQLConvert(sqls)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error SQLConvert the client!"})
    		w.Write(msg)
    		return 
    	}
    	res, err := es.Search(
    		es.Search.WithContext(context.Background()),
    		es.Search.WithIndex(table),
    		es.Search.WithBody(strings.NewReader(dsl)),
    		es.Search.WithTrackTotalHits(true),
    		es.Search.WithPretty(),
    	)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error Search !"})
    		w.Write(msg)
    		return 
    	}
    	defer res.Body.Close()
    
    	if res.IsError() {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error res!"})
    		w.Write(msg)
    		return 
    	}
    	var r map[string]interface{}
    	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " Error parsing the response body!"})
    		w.Write(msg)
    		return 
    	}
    	
    	// Print the ID and document source for each hit.
    	resutData := make([](map[string]interface{}), 0)
    	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
    		// hit.(map[string]interface{})["_source"]
    		// hit.(map[string]interface{})["_id"]
    		s_source :=  hit.(map[string]interface{})["_source"]
    		
    		//重复转一次
    		resutData = append(resutData, s_source.(map[string]interface{}))
    	}
    	msg, _ := json.Marshal(JsonRes{Code: 200, Data: resutData})
    	w.Write(msg)
    	return
    }
    
    if d=="elasticsearch"{
    	cfg := elasticsearch.Config{
    		Addresses: []string{
    			"http://"+s,
    		},
    		Transport: &http.Transport{
    			MaxIdleConnsPerHost:   10,
    			ResponseHeaderTimeout: time.Second,
    			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
    			TLSClientConfig: &tls.Config{
    				MaxVersion:         tls.VersionTLS11,
    				InsecureSkipVerify: true,
    			},
    		},
    	}
    	es, err := elasticsearch.NewClient(cfg)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error creating the client!"})
    		w.Write(msg)
    		return 
    	}
        esql := elasticsql.NewElasticSQL()
        table, dsl, err := esql.SQLConvert(sqls)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error SQLConvert the client!"})
    		w.Write(msg)
    		return 
    	}
    	res, err := es.Search(
    		es.Search.WithContext(context.Background()),
    		es.Search.WithIndex(table),
    		es.Search.WithBody(strings.NewReader(dsl)),
    		es.Search.WithTrackTotalHits(true),
    		es.Search.WithPretty(),
    	)
    	if err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error Search !"})
    		w.Write(msg)
    		return 
    	}
    	defer res.Body.Close()
    
    	if res.IsError() {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " 404 Error res!"})
    		w.Write(msg)
    		return 
    	}
    	var r map[string]interface{}
    	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
    		w.WriteHeader(404)
    		msg, _ := json.Marshal(&JsonRes{Code: 4002, Msg: " Error parsing the response body!"})
    		w.Write(msg)
    		return 
    	}
    	
    	//组装r
    	msg, _ := json.Marshal(JsonRes{Code: 200, Data: r["hits"]})
    	fmt.Println(cast.ToString(123464))
    	w.Write(msg)
    	return
    	
    	/*
    	// Print the ID and document source for each hit.
    	resutData := make([](map[string]interface{}), 0)
    	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
    		// hit.(map[string]interface{})["_source"]
    		// hit.(map[string]interface{})["_id"]
    		s_source :=  hit.(map[string]interface{})["_source"]
    		
    		//重复转一次
    		resutData = append(resutData, s_source.(map[string]interface{}))
    	}
    	msg, _ := json.Marshal(JsonRes{Code: 200, Data: resutData})
    	w.Write(msg)
    	return
    	*/
    }
    
    
    
    
    
    if d=="oracle"{
        // 	db, err := gorm.Open(oracle.Open("oracle://H2:hydeesoft@192.168.9.3:1521/hydee"), &gorm.Config{})
    	db, err := gorm.Open(oracle.Open(s), &gorm.Config{})
    	if err != nil {
    		panic(err)
    	}
    	// 5秒内连接没有活跃的话则自动关闭连接
    // 	db.SetConnMaxLifetime(time.Second * 5)
    	// rows, err := db.Raw("SELECT A .busno AS busno, NVL ( v_busno_class_set_03大.classname, '未划分' ) AS compname, NVL ( v_busno_class_set_03小.classname, '未划分' ) AS area, f_get_orgname (A .busno) AS orgname, SUM ( ROUND ( ( A .netprice * A .wareqty * A . TIMES + A .minqty * A . TIMES * A .minprice ), 2 ) ) AS netsum, COUNT (DISTINCT A .saleno) AS kll, COUNT (A .saleno) AS xscs, 1 AS days, SUM ( ROUND ( NVL ( ROUND ( ( ( CASE WHEN b.limitprice = 0 OR b.limitprice IS NULL THEN A .purprice ELSE b.limitprice END ) * ( A .wareqty + ( CASE WHEN A .stdtomin = 0 THEN 0 ELSE A .minqty / A .stdtomin END ) ) * A . TIMES ), 6 ), ROUND ( ( i.purprice * ( A .wareqty + ( CASE WHEN A .stdtomin = 0 THEN 0 ELSE A .minqty / A .stdtomin END ) ) * A . TIMES ), 6 ) ), 6 ) ) AS puramt FROM t_area r1, t_factory f, t_sale_d A LEFT JOIN t_store_i i ON A .wareid = i.wareid AND A .batid = i.batid, t_sale_h c, t_ware b, v_busno_class_set_big v_busno_class_set_03大, v_busno_class_set_mid v_busno_class_set_03中, v_busno_class_set v_busno_class_set_03小 WHERE b.wareid = A .wareid AND b.compid = c.compid AND c.saleno = A .saleno AND f.factoryid = b.factoryid AND r1.areacode = b.areacode AND A .busno = c.busno AND A .accdate = c.accdate AND ( ( 2 = 0 AND c.compid IN ( SELECT compid FROM s_user WHERE userid = 168 ) ) OR (c.compid = 2) ) AND c.busno IN ( SELECT busno FROM s_user_busi WHERE userid = 168 AND status = 1 ) AND EXISTS ( SELECT * FROM s_user_busi x WHERE A .busno = x.busno AND ((2 <> 0 AND x.compid = 2) OR(2 = 0)) AND x.userid = 168 AND x.status = 1 ) AND EXISTS ( SELECT * FROM T_WARE_CLASS_BASE wc__ WHERE wc__.compid = ( CASE WHEN EXISTS ( SELECT 1 FROM t_ware_class_base twcb WHERE twcb.compid = 2 AND twcb.wareid = b.wareid ) THEN 2 ELSE 0 END ) AND wc__.WAREID = b.wareid AND wc__.CLASSGROUPNO = '42' AND SUBSTR (wc__.CLASSCODE, 1, 4) = '4201' ) AND b.warekind <> 3 AND v_busno_class_set_03大.classgroupno = '03' AND v_busno_class_set_03大.BUSNO = c.busno AND v_busno_class_set_03大.compid = 2 AND v_busno_class_set_03中.classgroupno = '03' AND v_busno_class_set_03中.BUSNO = c.busno AND v_busno_class_set_03中.compid = 2 AND v_busno_class_set_03小.classgroupno = '03' AND v_busno_class_set_03小.BUSNO = c.busno AND v_busno_class_set_03小.compid = 2 AND A .saler <> 802 AND ( A .accdate = TO_DATE ('2023-02-01', 'yyyy-MM-dd' ) ) GROUP BY A .busno, NVL ( v_busno_class_set_03大.classname, '未划分' ), NVL ( v_busno_class_set_03中.classname, '未划分' ), NVL ( v_busno_class_set_03小.classname, '未划分' )").Rows()
    	rows, err = db.Raw(sqls).Rows()
    	if err != nil {
    		panic(err)
    	}
    	//关闭连接
        sqlDB, err := db.DB()
    	if err != nil {
    		panic(err)
    	}
        defer sqlDB.Close()
    }else{
		db, err := sql.Open(d, s)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		rows, err = db.Query(sqls)
		if err != nil {
			panic(err)
		}
    }


	defer rows.Close()
	cols, _ := rows.Columns()
	colsize := len(cols)

	resutData := make([](map[string]interface{}), 0)
	for rows.Next() {
		colsjson := make(map[string]interface{}, colsize)
		colmeta := make([]interface{}, colsize)
		for i := 0; i < colsize; i++ {
			colmeta[i] = new(interface{})
		}
		rows.Scan(colmeta...)
		for i := 0; i < colsize; i++ {
			v := colmeta[i].(*interface{})
			var c string
			switch (*v).(type) {
			case nil:
				c = ""
			case float64, float32:
				c = fmt.Sprintf("%v", *v)
			case int64, int32, int16:
				c = fmt.Sprintf("%v", *v)
			default:
				c = fmt.Sprintf("%s", *v)
			}
			colsjson[strings.ToLower(cols[i])] = c
		}
		resutData = append(resutData, colsjson)
		// fmt.Println(args)
	}
	msg, _ := json.Marshal(JsonRes{Code: 200, Data: resutData})
	w.Write(msg)
	return
}

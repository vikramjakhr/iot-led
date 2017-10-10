package main

import (
	"fmt"
	"log"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"reflect"
	"strings"
)

func main() {
	http.HandleFunc("/sensor/data", Data)
	//Starting server
	log.Println("Starting Server on port 9090")
	http.ListenAndServe(":9090", nil)
}

func Data(w http.ResponseWriter, req *http.Request) {
	field := strings.ToUpper(req.URL.Query().Get("field"))
	resp, err := GetData()
	if err != nil {
		log.Println("Error occured while fetching data from influxdb")
		return
	}
	values := resp.Results[0].Series[0].Values[0]
	p := Payload{
		BATLEVEL:  values[1],
		CO:        values[2],
		NO2:       values[3],
		O3:        values[4],
		PM1:       values[5],
		PM10:      values[6],
		PM2_5:     values[7],
		SO2:       values[8],
		TIMESTAMP: values[9],
		HUM:       values[10],
		PRE:       values[10],
		TEMP:      values[10],
	}
	if field != "" {
		r := reflect.ValueOf(p)
		f := reflect.Indirect(r).FieldByName(field)
		m := make(map[string]interface{})
		m[field] = f.Interface()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m)
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	}

}

type Response struct {
	Results []Object `json:"results"`
}

type Object struct {
	StatementId int `json:"statement_id"`
	Series      []SeriesItem `json:"series"`
}

type SeriesItem struct {
	Name    string `json:"name"`
	Columns []string `json:"columns"`
	Values  [][]interface{} `json:"values"`
}

type Payload struct {
	TIMESTAMP interface{} `json:"TIMESTAMP"`
	CO        interface{} `json:"CO"`
	TEMP      interface{} `json:"temp"`
	HUM       interface{} `json:"hum"`
	PRE       interface{} `json:"pre"`
	O3        interface{} `json:"O3"`
	SO2       interface{} `json:"SO2"`
	NO2       interface{} `json:"NO2"`
	PM1       interface{} `json:"PM1"`
	PM2_5     interface{} `json:"PM2_5"`
	PM10      interface{} `json:"PM10"`
	BATLEVEL  interface{} `json:"BATLEVEL"`
}

func GetData() (*Response, error) {
	resp := &Response{}
	params := make(map[string]interface{})
	params["pretty"] = true
	params["db"] = "ioi"
	params["q"] = `SELECT last(BATLEVEL) as BATLEVEL, last(CO) as CO, last(NO2) as NO2, last(O3) as O3, last(PM1) as PM1, last(PM10) as PM10, last(PM2_5) as PM2_5, last(SO2) as SO2, last(TIMESTAMP) as TIMESTAMP, last(hum) as hum, last(pre) as pre, last(temp) as temp FROM sensor;`
	byts, err := HTTPGet("http://34.233.194.217:8086/query", params)
	if err != nil {
		log.Println(fmt.Sprintf("Error while http to influx"))
		return resp, err
	}
	err = json.Unmarshal(byts, resp)
	if err != nil {
		log.Println(err)
		return resp, err
	}
	return resp, nil
}

func HTTPGet(url string, params map[string]interface{}) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	query := req.URL.Query()
	if params != nil {
		for k, v := range params {
			query.Set(k, fmt.Sprintf("%v", v))
		}
	}
	req.URL.RawQuery = query.Encode()
	log.Println("GET Request :" + req.URL.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	read, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, err
	}
	return read, err
}

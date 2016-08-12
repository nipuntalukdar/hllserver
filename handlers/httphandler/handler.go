package httphandler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/nipuntalukdar/hllserver/hll"
	"io"
	"net/http"
	"strconv"
)

const (
	mAXUPDLENGTH = 1024 * 1024 * 10
)

type HttpAddLogHandler struct {
	hlc     *hll.HllContainer
	allowed []string
}

type HttpDelLogHandler struct {
	hlc     *hll.HllContainer
	allowed []string
}

type HttpUpdateLogHandler struct {
	hlc     *hll.HllContainer
	allowed []string
}

type HttpGetCardinalityHandler struct {
	hlc     *hll.HllContainer
	allowed []string
}

func NewHttpAddLogHandler(hlc *hll.HllContainer) *HttpAddLogHandler {
	return &HttpAddLogHandler{hlc: hlc, allowed: []string{http.MethodGet}}
}

func NewHttpDelLogHandler(hlc *hll.HllContainer) *HttpDelLogHandler {
	return &HttpDelLogHandler{hlc: hlc, allowed: []string{http.MethodGet}}
}

func NewHttpUpdateLogHandler(hlc *hll.HllContainer) *HttpUpdateLogHandler {
	return &HttpUpdateLogHandler{hlc: hlc, allowed: []string{http.MethodPost}}
}

func NewHttpGetCardinalityHandler(hlc *hll.HllContainer) *HttpGetCardinalityHandler {
	return &HttpGetCardinalityHandler{hlc: hlc, allowed: []string{http.MethodGet}}
}

func checkMethod(req *http.Request, w http.ResponseWriter, allowedMethods []string) bool {
	for _, method := range allowedMethods {
		if req.Method == method {
			return true
		}
	}
	w.WriteHeader(http.StatusBadRequest)
	jsonm := map[string]string{"status": "failure", "msg": "Unsupported method"}
	jdata, _ := json.Marshal(jsonm)
	w.Header().Set("Content-type", "application/json")
	w.Write(jdata)
	return false
}

func failureStatus(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	jsonm := map[string]string{"status": "failure", "msg": msg}
	jdata, _ := json.Marshal(jsonm)
	w.Header().Set("Content-type", "application/json")
	w.Write(jdata)
}

func successStatus(w http.ResponseWriter) {
	jsonm := map[string]string{"status": "success"}
	jdata, _ := json.Marshal(jsonm)
	w.Header().Set("Content-type", "application/json")
	w.Write(jdata)
}

func checkLogKey(req *http.Request, w http.ResponseWriter) string {
	req.ParseForm()
	data := req.Form
	logkeys, ok := data["logkey"]
	if !ok {
		failureStatus(w, http.StatusBadRequest, "logkey is missing")
		return ""
	}
	if len(logkeys) != 1 || len(logkeys[0]) == 0 {
		failureStatus(w, http.StatusBadRequest,
			"logkey must have one and only one non-empty value")
		return ""
	}
	return logkeys[0]
}

func (hl *HttpAddLogHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !checkMethod(req, w, hl.allowed) {
		return
	}
	logkey := checkLogKey(req, w)
	if logkey == "" {
		return
	}
	data := req.Form
	expiry, ok := data["expiry"]
	expiry_time := int64(0)
	var err error
	if ok {
		if len(expiry) > 1 {
			failureStatus(w, http.StatusBadRequest, "multiple values for expiry")
			return
		}
		if len(expiry) != 0 {
			expiry_time, err = strconv.ParseInt(expiry[0], 10, 64)
			if err != nil {
				failureStatus(w, http.StatusBadRequest, "Invalid values for expiry")
				return
			}
		}
	}
	fmt.Printf("Expiry time %d\n", expiry_time)
	hl.hlc.AddLog(logkey, nil)
	successStatus(w)
}

func (hl *HttpDelLogHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !checkMethod(req, w, hl.allowed) {
		return
	}
	logkey := checkLogKey(req, w)
	if logkey == "" {
		return
	}
	hl.hlc.DelLog(logkey)
	successStatus(w)
}

func (hl *HttpUpdateLogHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !checkMethod(req, w, hl.allowed) {
		return
	}
	// Now read the json of update request
	dlen := int(req.ContentLength)
	if dlen > mAXUPDLENGTH || dlen == 0 {
		failureStatus(w, http.StatusBadRequest, "Invalid length for updatelog handler")
		return
	}
	body := make([]byte, dlen)
	read := 0
	for {
		l, err := req.Body.Read(body[read:])
		if err != nil && err != io.EOF {
			failureStatus(w, http.StatusBadRequest, "Couldn't read the request body completely")
			return
		}
		read += l
		if read == dlen {
			break
		}
	}
	var decoded map[string]interface{}
	err := json.Unmarshal(body, &decoded)
	if err != nil {
		failureStatus(w, http.StatusBadRequest, "Couldn't decode json data")
		return
	}
	keyi, ok := decoded["logkey"]
	if !ok {
		failureStatus(w, http.StatusBadRequest, "Logkey is missing")
		return
	}
	logkey := keyi.(string)
	vals, ok := decoded["values"]
	if !ok {
		failureStatus(w, http.StatusBadRequest, "values are missing")
		return
	}
	valsb64 := vals.([]interface{})
	bindata := make([][]byte, len(valsb64))
	for i, val := range valsb64 {
		bindt, err := base64.StdEncoding.DecodeString(val.(string))
		if err != nil {
			failureStatus(w, http.StatusBadRequest, "Base64 decode problem")
			return
		}
		bindata[i] = bindt
	}
	hl.hlc.AddMLog(logkey, bindata)
	successStatus(w)
}

func (hl *HttpGetCardinalityHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !checkMethod(req, w, hl.allowed) {
		return
	}
	logkey := checkLogKey(req, w)
	if logkey == "" {
		return
	}
	card := hl.hlc.GetCardinality(logkey)
	jsonm := map[string]interface{}{"status": "success", "cardinality": card}
	jdata, _ := json.Marshal(jsonm)
	w.Header().Set("Content-type", "application/json")
	w.Write(jdata)
}

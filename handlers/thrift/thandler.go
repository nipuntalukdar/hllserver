package thandler

import (
	"encoding/gob"
	"github.com/nipuntalukdar/hllserver/hll"
	"github.com/nipuntalukdar/hllserver/hllthrift"
)

type ThriftHandler struct {
	hlc *hll.HllContainer
}

func registerTypes() {
	gob.Register(hllthrift.NewAddLogCmd())
	gob.Register(hllthrift.NewUpdateLogCmd())
	gob.Register(hllthrift.NewUpdateLogMValCmd())
	gob.Register(hllthrift.NewUpdateExpiryCmd())
	gob.Register(hllthrift.NewCardinalityResponse())
}

func init() {
	registerTypes()
}

func NewThriftHandler(hlc *hll.HllContainer) (*ThriftHandler, error) {
	if hlc == nil {
		panic("Container is nil")
	}
	return &ThriftHandler{hlc: hlc}, nil
}

func (th *ThriftHandler) AddLog(add *hllthrift.AddLogCmd) (hllthrift.Status, error) {
	th.hlc.AddLog(add.Key, nil)
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) UpdateExpiry(upde *hllthrift.UpdateExpiryCmd) (hllthrift.Status, error) {
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) Update(updl *hllthrift.UpdateLogCmd) (hllthrift.Status, error) {
	th.hlc.AddLog(updl.Key, updl.Data)
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) UpdateM(updlm *hllthrift.UpdateLogMValCmd) (hllthrift.Status, error) {
	th.hlc.AddMLog(updlm.Key, updlm.Data)
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) DelLog(key string) (hllthrift.Status, error) {
	th.hlc.DelLog(key)
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) GetCardinality(key string) (*hllthrift.CardinalityResponse, error) {
	card := th.hlc.GetCardinality(key)
	r := hllthrift.NewCardinalityResponse()
	r.Status = hllthrift.Status_SUCCESS
	r.Key = key
	r.Cardinality = int64(card)
	return r, nil
}

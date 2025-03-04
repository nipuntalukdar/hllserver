package thandler

import (
	"context"
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

func (th *ThriftHandler) AddLog(ctx context.Context, add *hllthrift.AddLogCmd) (hllthrift.Status, error) {
	th.hlc.AddLog(add.Key, nil, uint64(add.Expiry))
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) UpdateExpiry(ctx context.Context, upde *hllthrift.UpdateExpiryCmd) (hllthrift.Status, error) {
	if th.hlc.UpdateExpiry(upde.Key, uint64(upde.Expiry)) {
		return hllthrift.Status_SUCCESS, nil
	} else {
		return hllthrift.Status_FAILURE, nil
	}
}

func (th *ThriftHandler) Update(ctx context.Context, updl *hllthrift.UpdateLogCmd) (hllthrift.Status, error) {
	th.hlc.AddLog(updl.Key, updl.Data, uint64(updl.Expiry))
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) UpdateM(ctx context.Context, updlm *hllthrift.UpdateLogMValCmd) (hllthrift.Status, error) {
	th.hlc.AddMLog(updlm.Key, updlm.Data, uint64(updlm.Expiry))
	return hllthrift.Status_SUCCESS, nil
}

func (th *ThriftHandler) DelLog(ctx context.Context, key string) (hllthrift.Status, error) {
	ret := th.hlc.DelLog(key)
	if ret {
		return hllthrift.Status_SUCCESS, nil
	} else {
		return hllthrift.Status_FAILURE, nil
	}
}

func (th *ThriftHandler) GetCardinality(ctx context.Context, key string) (*hllthrift.CardinalityResponse, error) {
	card := th.hlc.GetCardinality(key)
	r := hllthrift.NewCardinalityResponse()
	r.Status = hllthrift.Status_SUCCESS
	r.Key = key
	r.Cardinality = int64(card)
	return r, nil
}

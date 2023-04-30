package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type remoteLabelableQueryable struct {
	storage.Queryable
	lbUrl string
}

func (q *remoteLabelableQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	qr, err := q.Queryable.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &remoteLabelableQueryer{Querier: qr, lbUrl: q.lbUrl, mint: float64(mint), maxt: float64(maxt)}, nil
}

type remoteLabelableQueryer struct {
	storage.Querier
	lbUrl      string
	mint, maxt float64
}

type labelValuesResp struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func (q *remoteLabelableQueryer) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	params := url.Values{}
	params.Add("start", strconv.FormatFloat(q.mint/1000, 'f', 0, 64))
	params.Add("end", strconv.FormatFloat(q.maxt/1000, 'f', 0, 64))
	for _, m := range matchers {
		params.Add("match[]", m.String())
	}
	uri := fmt.Sprint(q.lbUrl, "/", name, "/values?", params.Encode())
	resp, err := http.Get(uri)
	if err != nil {
		return nil, nil, errors.Wrap(err, "http.Get.LabelValues")
	}
	defer resp.Body.Close()
	val := &labelValuesResp{}
	if err := json.NewDecoder(resp.Body).Decode(val); err != nil {
		return nil, nil, err
	}
	return val.Data, nil, nil
}

func NewLabelableQueryable(q storage.Queryable, lbUrl string) storage.Queryable {
	return &remoteLabelableQueryable{Queryable: q, lbUrl: strings.TrimSuffix(lbUrl, "/")}
}

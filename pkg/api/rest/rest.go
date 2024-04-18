// Copyright 2024 The Solaris Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rest

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	restapi "github.com/solarisdb/solaris/api/genpublic/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/api"
	"net/http"
)

type (
	ErrorMsg struct {
		Error string `json:"error"`
	}

	Rest struct {
		svc    *api.Service
		logger logging.Logger
	}
)

var _ restapi.ServerInterface = (*Rest)(nil)

func New(svc *api.Service) *Rest {
	return &Rest{svc: svc, logger: logging.NewLogger("api.rest")}
}

func (r *Rest) RegisterEPs(g *gin.Engine) error {
	restapi.RegisterHandlersWithOptions(g, r, restapi.GinServerOptions{BaseURL: "v1"})
	return nil
}

func (r *Rest) CreateLog(c *gin.Context) {
	var rReq restapi.CreateLogRequest
	if r.errorResponse(c, BindAppJson(c, &rReq), "") {
		return
	}
	sLog, err := r.svc.CreateLog(c, &solaris.Log{Tags: rReq.Tags})
	if r.errorResponse(c, err, "") {
		return
	}
	c.JSON(http.StatusCreated, logToRest(sLog))
}

func (r *Rest) UpdateLog(c *gin.Context, logId restapi.LogId) {
	var rReq restapi.UpdateLogRequest
	if r.errorResponse(c, BindAppJson(c, &rReq), "") {
		return
	}
	sLog, err := r.svc.UpdateLog(c, &solaris.Log{ID: logId, Tags: rReq.Tags})
	if r.errorResponse(c, err, "") {
		return
	}
	c.JSON(http.StatusOK, logToRest(sLog))
}

func (r *Rest) DeleteLogs(c *gin.Context) {
	var rReq restapi.DeleteLogsRequest
	if r.errorResponse(c, BindAppJson(c, &rReq), "") {
		return
	}
	sRes, err := r.svc.DeleteLogs(c, &solaris.DeleteLogsRequest{Condition: rReq.FilterCondition})
	if r.errorResponse(c, err, "") {
		return
	}
	c.JSON(http.StatusOK, restapi.DeleteLogsResponse{Deleted: len(sRes.DeletedIDs)})
}

func (r *Rest) QueryLogs(c *gin.Context, params restapi.QueryLogsParams) {
	sReq := new(solaris.QueryLogsRequest)
	sReq.Condition = cast.String(params.LogsCondFilter, "")
	sReq.Limit = int64(cast.Int(params.Limit, 0))
	sReq.PageID = cast.String(params.FromPageId, "")

	sRes, err := r.svc.QueryLogs(c, sReq)
	if r.errorResponse(c, err, "") {
		return
	}
	rRes := restapi.QueryLogsResult{Items: logsToRest(sRes.Logs), Total: int(sRes.Total)}
	if len(sRes.NextPageID) > 0 {
		rRes.NextPageId = cast.Ptr(sRes.NextPageID)
	}
	c.JSON(http.StatusOK, rRes)
}

func (r *Rest) CreateRecords(c *gin.Context, logId restapi.LogId) {
	var rReq restapi.CreateRecordsRequest
	if r.errorResponse(c, BindAppJson(c, &rReq), "") {
		return
	}
	sReq := new(solaris.AppendRecordsRequest)
	sReq.LogID = logId
	sReq.Records = createRecsToSvc(rReq.Records)
	sRes, err := r.svc.AppendRecords(c, sReq)
	if r.errorResponse(c, err, "") {
		return
	}
	c.JSON(http.StatusCreated, restapi.CreateRecordsResponse{Added: int(sRes.Added)})
}

func (r *Rest) QueryRecords(c *gin.Context, params restapi.QueryRecordsParams) {
	sReq := new(solaris.QueryRecordsRequest)
	sReq.LogsCondition = cast.String(params.LogsCondFilter, "")
	sReq.Condition = cast.String(params.RecordsCondFilter, "")
	sReq.LogIDs = cast.Value(params.LogIds, nil)
	sReq.Descending = cast.Bool(params.Desc, false)
	sReq.StartRecordID = cast.String(params.FromPageId, "")
	sReq.Limit = int64(cast.Int(params.Limit, 0))

	sResQ, err := r.svc.QueryRecords(c, sReq)
	if r.errorResponse(c, err, "") {
		return
	}
	sResC, err := r.svc.CountRecords(c, sReq)
	if r.errorResponse(c, err, "") {
		return
	}
	rRes := restapi.QueryRecordsResult{Items: recsToRest(sResQ.Records), Total: int(sResC.Count)}
	if len(sResQ.NextPageID) > 0 {
		rRes.NextPageId = cast.Ptr(sResQ.NextPageID)
	}
	c.JSON(http.StatusOK, rRes)
}

func (r *Rest) Ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func (r *Rest) errorResponse(c *gin.Context, err error, msg string) bool {
	if err == nil {
		return false
	}
	if msg == "" {
		msg = err.Error()
	}
	status := http.StatusInternalServerError
	defer func() {
		c.JSON(status, ErrorMsg{Error: msg})
		r.logger.Warnf("%s %s -> %d %s", c.Request.Method, c.Request.URL, status, msg)
		r.logger.Debugf("original error: %s", err)
	}()

	if errors.Is(err, errors.ErrNotExist) {
		status = http.StatusNotFound
	} else if errors.Is(err, errors.ErrInvalid) {
		status = http.StatusBadRequest
	} else if errors.Is(err, errors.ErrExist) {
		status = http.StatusConflict
	} else if errors.Is(err, errors.ErrConflict) {
		status = http.StatusConflict
	}
	return true
}

// BindAppJson turns the request body to inf, but for "application/json" contents only
func BindAppJson(c *gin.Context, inf interface{}) error {
	ct := c.ContentType()
	if ct != "application/json" {
		return fmt.Errorf("invalid content type %s, \"application/json\" expected. err=%w", ct, errors.ErrInternal)
	}
	err := c.Bind(inf)
	if err != nil {
		err = fmt.Errorf("%s: %w", err.Error(), errors.ErrInternal)
	}
	return err
}

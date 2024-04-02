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
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	restapi "github.com/solarisdb/solaris/api/genpublic/v1"
)

func logToRest(sLog *solaris.Log) restapi.Log {
	var rLog restapi.Log
	rLog.Id = sLog.ID
	rLog.Tags = sLog.Tags
	rLog.Records = int(sLog.Records)
	if sLog.CreatedAt != nil {
		rLog.CreatedAt = sLog.CreatedAt.AsTime()
	}
	if sLog.UpdatedAt != nil {
		rLog.UpdatedAt = sLog.UpdatedAt.AsTime()
	}
	return rLog
}

func logsToRest(sLogs []*solaris.Log) []restapi.Log {
	var rLogs []restapi.Log
	for _, l := range sLogs {
		rLogs = append(rLogs, logToRest(l))
	}
	return rLogs
}

func createRecToSvc(rRec restapi.CreateRecordRequest) *solaris.Record {
	sRec := new(solaris.Record)
	sRec.Payload = rRec.Payload
	return sRec
}

func createRecsToSvc(rRecs []restapi.CreateRecordRequest) []*solaris.Record {
	var sRecs []*solaris.Record
	for _, r := range rRecs {
		sRecs = append(sRecs, createRecToSvc(r))
	}
	return sRecs
}

func recToRest(sRec *solaris.Record) restapi.Record {
	var rRec restapi.Record
	rRec.Id = sRec.ID
	rRec.LogId = sRec.LogID
	rRec.Payload = sRec.Payload
	if sRec.CreatedAt != nil {
		rRec.CreatedAt = sRec.CreatedAt.AsTime()
	}
	return rRec
}

func recsToRest(sRecs []*solaris.Record) []restapi.Record {
	var rRecs []restapi.Record
	for _, r := range sRecs {
		rRecs = append(rRecs, recToRest(r))
	}
	return rRecs
}

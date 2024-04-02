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

package server

import (
	"context"
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/api"
	"github.com/solarisdb/solaris/pkg/api/rest"
	"github.com/solarisdb/solaris/golibs/sss/inmem"
	"github.com/solarisdb/solaris/pkg/grpc"
	"github.com/solarisdb/solaris/pkg/http"
	"github.com/solarisdb/solaris/pkg/storage/buntdb"
	"github.com/solarisdb/solaris/pkg/storage/cache"
	"github.com/solarisdb/solaris/pkg/storage/chunkfs"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"github.com/solarisdb/solaris/pkg/version"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/davecgh/go-spew/spew"
	"github.com/logrange/linker"
	ggrpc "google.golang.org/grpc"
)

// Run is an entry point of the Solaris server
func Run(ctx context.Context, cfg *Config) error {
	log := logging.NewLogger("server")
	log.Infof("starting server: %s", version.BuildVersionString())

	log.Infof(spew.Sprint(cfg))
	defer log.Infof("server is stopped")

	if err := checkConfig(cfg); err != nil {
		return err
	}

	// gRPC server
	gsvc := api.NewService()
	var grpcRegF grpc.RegisterF = func(gs *ggrpc.Server) error {
		grpc_health_v1.RegisterHealthServer(gs, health.NewServer())
		solaris.RegisterServiceServer(gs, gsvc)
		return nil
	}

	// Http API (endpoints)
	rst := rest.New(gsvc)

  // chunksfs
	provider := chunkfs.NewProvider(cfg.LocalDBFilePath, cfg.MaxOpenedLogFiles, chunkfs.GetDefaultConfig())
	replicator := chunkfs.NewReplicator(provider.GetFileNameByID)

	inj := linker.New()
	inj.Register(linker.Component{Name: "", Value: cache.NewCachedStorage(buntdb.NewStorage(buntdb.Config{DBFilePath: cfg.MetaDBFilePath}))})
	inj.Register(linker.Component{Name: "", Value: provider})
	inj.Register(linker.Component{Name: "", Value: chunkfs.NewChunkAccessor()})
	inj.Register(linker.Component{Name: "", Value: replicator})
	inj.Register(linker.Component{Name: "", Value: chunkfs.NewScanner(replicator, chunkfs.GetDefaultScannerConfig())})
	inj.Register(linker.Component{Name: "", Value: inmem.NewStorage()})
	inj.Register(linker.Component{Name: "", Value: logfs.NewLocalLog(logfs.GetDefaultConfig())})
	inj.Register(linker.Component{Name: "", Value: gsvc})
	inj.Register(linker.Component{Name: "", Value: grpc.NewServer(grpc.Config{Transport: *cfg.GrpcTransport, RegisterEndpoints: grpcRegF})})
	inj.Register(linker.Component{Name: "", Value: http.NewRouter(http.Config{HttpPort: cfg.HttpPort, RestRegistrar: rst.RegisterEPs})})

	inj.Init(ctx)
	<-ctx.Done()
	inj.Shutdown()
	return nil
}

func checkConfig(cfg *Config) error {
	if cfg.LocalDBFilePath == "" {
		return fmt.Errorf("LocalDBFilePath must be provided: %w", errors.ErrInvalid)
	}
	return files.EnsureDirExists(cfg.LocalDBFilePath)
}

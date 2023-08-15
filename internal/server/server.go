package server

import (
	"context"
	api "github.com/martin-lenweiter/proglog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, request *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(request.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}
func (s *grpcServer) Consume(ctx context.Context, request *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(request.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream implements a bidirectional streaming
// RPC so the client can stream data into the server’s log and the server can tell
// the client whether each request succeeded.
// 1. Wait for a request from the client: req, err := stream.Recv().
// 2. If there's an error in receiving, return the error.
// 3. Once the request is received, the server processes it with s.Produce(stream.Context(), req).
// 4. If the produce method encounters an error, it is returned.
// 5. If the produce method succeeds, its result res is sent back to the client: stream.Send(res).
// 6. If there's an error in sending the response, return the error.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream implements a server-side streaming RPC so the
// client can tell the server where in the log to read records, and then the server
// will stream every record that follows—even records that aren’t in the log yet!
// When the server reaches the end of the log, the server will wait until someone
// appends a record to the log and then continue streaming records to the client.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:

			case api.ErrOffsetOutOfRange:
				continue

			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

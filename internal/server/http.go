package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
)

func NewHttpServer(addr string) *http.Server {
	server := newHttpServer()
	r := mux.NewRouter()
	r.HandleFunc("/", server.handleProduce).Methods("POST")
	r.HandleFunc("/", server.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func (s httpServer) handleProduce(writer http.ResponseWriter, request *http.Request) {
	// deserialize
	var produceRequest ProduceRequest
	err := json.NewDecoder(request.Body).Decode(&produceRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// perform operation
	record := produceRequest.Record
	offset, err := s.Log.Append(record)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	// serialize
	produceResponse := ProduceResponse{Offset: offset}
	encoder := json.NewEncoder(writer)
	err = encoder.Encode(produceResponse)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (s httpServer) handleConsume(writer http.ResponseWriter, request *http.Request) {
	// deserialize
	var consumeRequest ConsumeRequest
	err := json.NewDecoder(request.Body).Decode(&consumeRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
	}

	// perform operation
	offset := consumeRequest.Offset
	record, err := s.Log.Read(offset)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	// serialize
	consumeResponse := ConsumeResponse{Record: record}
	encoder := json.NewEncoder(writer)
	err = encoder.Encode(consumeResponse)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func newHttpServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

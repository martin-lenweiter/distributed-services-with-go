compile:
	protoc api/v1/*.proto --go_out=. --go_opt=paths=source_relative --proto_path=.
	git add api/v1/*.pb.go

test:
	go test -race ./..
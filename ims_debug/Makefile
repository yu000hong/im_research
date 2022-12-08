all:ims

ims:storage_server.go storage_rpc.go rpc.go protocol.go message.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go monitoring.go
	go build -ldflags "-X main.Version=2.0.0 -X 'main.BuildTime=`date`' -X 'main.GoVersion=`go version`' -X 'main.GitCommitId=`git log --pretty=format:"%h" -1`' -X 'main.GitBranch=`git rev-parse --abbrev-ref HEAD`'" -o ims storage_server.go storage_rpc.go rpc.go protocol.go message.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go monitoring.go

clean:
	rm -f ims ims_trunncate main.test

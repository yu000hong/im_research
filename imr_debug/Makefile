all:imr

imr:route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go rpc.go
	go build -ldflags "-X main.Version=2.0.0 -X 'main.BuildTime=`date`' -X 'main.GoVersion=`go version`' -X 'main.GitCommitId=`git log --pretty=format:"%h" -1`' -X 'main.GitBranch=`git rev-parse --abbrev-ref HEAD`'" -o imr route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go rpc.go

clean:
	rm -f imr

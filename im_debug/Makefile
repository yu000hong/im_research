all:im

#dummy_grpc.go <=> grpc.go
im:im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go  storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go
	go build -ldflags "-X main.Version=2.0.0 -X 'main.BuildTime=`date`' -X 'main.GoVersion=`go version`' -X 'main.GitCommitId=`git log --pretty=format:"%h" -1`' -X 'main.GitBranch=`git rev-parse --abbrev-ref HEAD`'" im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go

clean:
	rm -f im

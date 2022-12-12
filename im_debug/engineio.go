package main

import (
	log "github.com/golang/glog"
	"github.com/googollee/go-engine.io"
	"io/ioutil"
	"net/http"
)

type EIOServer struct {
	server *engineio.Server
}

func (s *EIOServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Info(req.Header.Get("Origin"))
	if req.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", req.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", `Origin, No-Cache, X-Requested-With, If-Modified-Since, Pragma,
		Last-Modified, Cache-Control, Expires, Content-Type`)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}
	log.Info("req: %s", req)
	s.server.ServeHTTP(w, req)
}

func StartEngineIO(address string, tlsAddress string, certFile string, keyFile string) {
	server, err := engineio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			conn, err := server.Accept()
			if err != nil {
				log.Info("accept connect fail")
			}
			log.Info("new conn: %s", conn)
			client := NewClient(conn)
			client.Run()
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/ws", &EIOServer{server})
	log.Infof("EngineIO Serving at %s...", address)

	if tlsAddress != "" && certFile != "" && keyFile != "" {
		go func() {
			log.Infof("EngineIO Serving TLS at %s...", tlsAddress)
			err = http.ListenAndServeTLS(tlsAddress, certFile, keyFile, mux)
			if err != nil {
				log.Fatalf("listen err:%s", err)
			}
		}()
	}
	err = http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatalf("listen err:%s", err)
	}
}

func ReadEngineIOMessage(conn engineio.Conn) *Message {
	t, r, err := conn.NextReader()
	if err != nil {
		return nil
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil
	}
	_ = r.Close()
	if t == engineio.TEXT {
		return nil
	} else {
		return ReadBinaryMessage(b)
	}
}

func SendEngineIOBinaryMessage(conn engineio.Conn, msg *Message) {
	w, err := conn.NextWriter(engineio.BINARY)
	if err != nil {
		log.Info("get next writer fail")
		return
	}
	log.Info("message version:", msg.version)
	err = SendMessage(w, msg)
	if err != nil {
		log.Info("engine io write error")
		return
	}
	_ = w.Close()
}

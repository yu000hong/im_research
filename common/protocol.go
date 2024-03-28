package common

import "io"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import "errors"
import "encoding/hex"

func WriteHeader(len int32, seq int32, cmd byte, version byte, flag byte, buffer io.Writer) {
	_ = binary.Write(buffer, binary.BigEndian, len)
	_ = binary.Write(buffer, binary.BigEndian, seq)
	t := []byte{cmd, version, flag, 0}
	_, _ = buffer.Write(t)
}

func ReadHeader(buff []byte) (int, int, int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &length)
	_ = binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	version, _ := buffer.ReadByte()
	flag, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd), int(version), int(flag)
}

func WriteMessage(w *bytes.Buffer, msg *Message) {
	body := msg.ToData()
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), byte(msg.version), byte(msg.flag), w)
	w.Write(body)
}

func SendMessage(conn io.Writer, msg *Message) error {
	buffer := new(bytes.Buffer)
	WriteMessage(buffer, msg)
	buf := buffer.Bytes()
	n, err := conn.Write(buf)
	if err != nil {
		log.Info("sock write error:", err)
		return err
	}
	if n != len(buf) {
		log.Infof("write less:%d %d", n, len(buf))
		return errors.New("write less")
	}
	return nil
}

func ReceiveLimitMessage(conn io.Reader, limitSize int, external bool) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version, flag := ReadHeader(buff)
	if length < 0 || length >= limitSize {
		log.Info("invalid len:", length)
		return nil
	}

	//0 <= cmd <= 255
	//收到客户端非法消息，断开链接
	if external && !externalMessages[cmd] {
		log.Warning("invalid external message cmd:", Command(cmd))
		return nil
	}

	buff = make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	message := new(Message)
	message.cmd = cmd
	message.seq = seq
	message.version = version
	message.flag = flag
	if !message.FromData(buff) {
		log.Warningf("parse error:%d, %d %d %d %s", cmd, seq, version,
			flag, hex.EncodeToString(buff))
		return nil
	}
	return message
}

func ReceiveMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 32*1024, false)
}

// ReceiveClientMessage 接受客户端消息(external messages)
func ReceiveClientMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 32*1024, true)
}

// ReceiveStorageSyncMessage 消息大小限制在1M
func ReceiveStorageSyncMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 32*1024*1024, false)
}

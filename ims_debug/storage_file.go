package main

import (
	"im_research/ims_debug/lru"
	"os"
)
import "fmt"
import "bytes"
import "sync"
import "encoding/binary"
import "path/filepath"
import "strings"
import "strconv"
import "io"
import log "github.com/golang/glog"

const HeaderSize = 32
const Magic = 0x494d494d
const FVersion = 1 << 16 //1.0

const BlockSize = 128 * 1024 * 1024
const LruSize = 128

type StorageFile struct {
	root  string
	mutex sync.Mutex

	dirty   bool       //write file dirty
	blockNo int        //write file block NO
	file    *os.File   //write
	files   *lru.Cache //read, block files

	lastId      int64 //peer&group message_index记录的最大消息id
	lastSavedId int64 //索引文件中最大的消息id
}

func NewStorageFile(root string) *StorageFile {
	storage := new(StorageFile)
	storage.root = root
	storage.files = lru.New(LruSize)
	storage.files.OnEvicted = onFileEvicted

	//find the last block file
	pattern := fmt.Sprintf("%s/message_*", storage.root)
	files, _ := filepath.Glob(pattern)
	blockNo := 0 //begin from 0
	for _, f := range files {
		base := filepath.Base(f)
		if strings.HasPrefix(base, "message_") {
			if !checkFile(f) {
				log.Fatal("check file failure")
			} else {
				log.Infof("check file pass:%s", f)
			}
			b, err := strconv.ParseInt(base[8:], 10, 64)
			if err != nil {
				log.Fatal("invalid message file:", f)
			}

			if int(b) > blockNo {
				blockNo = int(b)
			}
		}
	}

	storage.openWriteFile(blockNo)

	return storage
}

//校验文件结尾是否合法
func checkFile(filePath string) bool {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("open file:", err)
	}

	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}

	if fileSize == HeaderSize {
		return true
	}

	if fileSize < HeaderSize {
		return false
	}

	_, err = file.Seek(fileSize-4, os.SEEK_SET)
	if err != nil {
		log.Fatal("seek file")
	}

	mf := make([]byte, 4)
	n, err := file.Read(mf)
	if err != nil || n != 4 {
		log.Fatal("read file err:", err)
	}
	buffer := bytes.NewBuffer(mf)
	var m int32
	binary.Read(buffer, binary.BigEndian, &m)
	return int(m) == Magic
}

//open write file
func (storage *StorageFile) openWriteFile(blockNo int) {
	path := fmt.Sprintf("%s/message_%d", storage.root, blockNo)
	log.Info("open/create message file path:", path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}
	if fileSize < HeaderSize && fileSize > 0 {
		log.Info("file header is't complete")
		err = file.Truncate(0)
		if err != nil {
			log.Fatal("truncate file")
		}
		fileSize = 0
	}
	if fileSize == 0 {
		storage.WriteHeader(file)
	}
	storage.file = file
	storage.blockNo = blockNo
	storage.dirty = false
}

func (storage *StorageFile) openReadFile(blockNo int) *os.File {
	//open file readonly mode
	path := fmt.Sprintf("%s/message_%d", storage.root, blockNo)
	log.Info("open message block file path:", path)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Infof("message block file:%s nonexist", path)
			return nil
		} else {
			log.Fatal(err)
		}
	}
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}
	if fileSize < HeaderSize && fileSize > 0 {
		if err != nil {
			log.Fatal("file header isn't complete")
		}
	}
	return file
}

func (storage *StorageFile) getMsgid(blockNo int, offset int) int64 {
	return int64(blockNo)*BlockSize + int64(offset)
}

func (storage *StorageFile) getBlockNo(msgid int64) int {
	return int(msgid / BlockSize)
}

func (storage *StorageFile) getBlockOffset(msgid int64) int {
	return int(msgid % BlockSize)
}

func (storage *StorageFile) getFile(blockNo int) *os.File {
	v, ok := storage.files.Get(blockNo)
	if ok {
		return v.(*os.File)
	}
	file := storage.openReadFile(blockNo)
	if file == nil {
		return nil
	}

	storage.files.Add(blockNo, file)
	return file
}

func (storage *StorageFile) ReadMessage(file *os.File) *Message {
	//校验消息起始位置的magic
	var magic int32
	err := binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}

	if magic != Magic {
		log.Warning("magic err:", magic)
		return nil
	}
	msg := ReceiveMessage(file)
	if msg == nil {
		return msg
	}

	err = binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}

	if magic != Magic {
		log.Warning("magic err:", magic)
		return nil
	}
	return msg
}

func (storage *StorageFile) LoadMessage(msgid int64) *Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	blockNo := storage.getBlockNo(msgid)
	offset := storage.getBlockOffset(msgid)

	file := storage.getFile(blockNo)
	if file == nil {
		log.Warning("can't get file object")
		return nil
	}

	_, err := file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		log.Warning("seek file")
		return nil
	}
	return storage.ReadMessage(file)
}

func (storage *StorageFile) ReadHeader(file *os.File) (magic int, version int) {
	header := make([]byte, HeaderSize)
	n, err := file.Read(header)
	if err != nil || n != HeaderSize {
		return
	}
	buffer := bytes.NewBuffer(header)
	var m, v int32
	binary.Read(buffer, binary.BigEndian, &m)
	binary.Read(buffer, binary.BigEndian, &v)
	magic = int(m)
	version = int(v)
	return
}

func (storage *StorageFile) WriteHeader(file *os.File) {
	var m int32 = Magic
	err := binary.Write(file, binary.BigEndian, m)
	if err != nil {
		log.Fatalln(err)
	}
	var v int32 = FVersion
	err = binary.Write(file, binary.BigEndian, v)
	if err != nil {
		log.Fatalln(err)
	}
	pad := make([]byte, HeaderSize-8)
	n, err := file.Write(pad)
	if err != nil || n != (HeaderSize-8) {
		log.Fatalln(err)
	}
}

func (storage *StorageFile) WriteMessage(file io.Writer, msg *Message) {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(Magic))
	WriteMessage(buffer, msg)
	binary.Write(buffer, binary.BigEndian, int32(Magic))
	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("file write err:", err)
	}
	if n != len(buf) {
		log.Fatal("file write size:", len(buf), " nwrite:", n)
	}
}

//save without lock
func (storage *StorageFile) saveMessage(msg *Message) int64 {
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(Magic))
	WriteMessage(buffer, msg)
	binary.Write(buffer, binary.BigEndian, int32(Magic))
	buf := buffer.Bytes()

	if msgid+int64(len(buf)) > BlockSize {
		err = storage.file.Sync()
		if err != nil {
			log.Fatalln("sync storage file:", err)
		}
		storage.file.Close()
		storage.openWriteFile(storage.blockNo + 1)
		msgid, err = storage.file.Seek(0, os.SEEK_END)
		if err != nil {
			log.Fatalln(err)
		}
	}

	if msgid+int64(len(buf)) > BlockSize {
		log.Fatalln("message size:", len(buf))
	}
	n, err := storage.file.Write(buf)
	if err != nil {
		log.Fatal("file write err:", err)
	}
	if n != len(buf) {
		log.Fatal("file write size:", len(buf), " nwrite:", n)
	}
	storage.dirty = true

	msgid = int64(storage.blockNo)*BlockSize + msgid
	master.ewt <- &EMessage{msgid: msgid, msg: msg}
	log.Info("save message:", Command(msg.cmd), " ", msgid)
	return msgid

}

func (storage *StorageFile) SaveMessage(msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.saveMessage(msg)
}

func (storage *StorageFile) Flush() {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	if storage.file != nil && storage.dirty {
		err := storage.file.Sync()
		if err != nil {
			log.Fatal("sync err:", err)
		}
		storage.dirty = false
		log.Info("sync storage file success")
	}
}

func onFileEvicted(key lru.Key, value interface{}) {
	f := value.(*os.File)
	_ = f.Close()
}

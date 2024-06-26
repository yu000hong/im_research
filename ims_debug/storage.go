package main

import "os"
import "bytes"

import log "github.com/golang/glog"

type Storage struct {
	*StorageFile
	*PeerStorage
}

func NewStorage(root string) *Storage {
	f := NewStorageFile(root)
	ps := NewPeerStorage(f)

	storage := &Storage{f, ps}

	r1 := storage.readPeerIndex()
	storage.lastSavedId = storage.lastId

	if r1 {
		storage.repairPeerIndex()
	} else {
		storage.createPeerIndex()
	}

	log.Infof("last id:%d last saved id:%d", storage.lastId, storage.lastSavedId)
	storage.FlushIndex()
	return storage
}

func (storage *Storage) NextMsgid() int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	offset, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	return offset + int64(storage.blockNo)*BlockSize
}

func (storage *Storage) execMessage(msg *Message, msgid int64) {
	storage.PeerStorage.execMessage(msg, msgid)
}

func (storage *Storage) ExecMessage(msg *Message, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	storage.execMessage(msg, msgid)
}

func (storage *Storage) SaveSyncMessageBatch(mb *MessageBatch) error {
	id := mb.firstId
	//all message come from one block
	for _, m := range mb.messages {
		emsg := &EMessage{id, 0, m}
		buffer := new(bytes.Buffer)
		storage.WriteMessage(buffer, m)
		id += int64(buffer.Len())
		storage.SaveSyncMessage(emsg)
	}

	log.Infof("save batch sync message first id:%d last id:%d\n", mb.firstId, mb.lastId)
	return nil
}

func (storage *Storage) SaveSyncMessage(emsg *EMessage) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	n := storage.getBlockNo(emsg.msgid)
	o := storage.getBlockOffset(emsg.msgid)

	if n < storage.blockNo || (n-storage.blockNo) > 1 {
		log.Warning("skip msg:", emsg.msgid)
		return nil
	}

	if (n - storage.blockNo) == 1 {
		storage.file.Close()
		storage.openWriteFile(n)
	}

	offset, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}

	if o < int(offset) {
		log.Warning("skip msg:", emsg.msgid)
		return nil
	} else if o > int(offset) {
		log.Warning("write padding:", o-int(offset))
		padding := make([]byte, o-int(offset))
		_, err = storage.file.Write(padding)
		if err != nil {
			log.Fatal("file write:", err)
		}
	}

	storage.WriteMessage(storage.file, emsg.msg)
	storage.execMessage(emsg.msg, emsg.msgid)
	log.Info("save sync message:", emsg.msgid)
	return nil
}

func (storage *Storage) LoadSyncMessagesInBackground(cursor int64) chan *MessageBatch {
	c := make(chan *MessageBatch, 10)
	go func() {
		defer close(c)

		blockNo := storage.getBlockNo(cursor)
		offset := storage.getBlockOffset(cursor)

		n := blockNo
		for {
			file := storage.openReadFile(n)
			if file == nil {
				break
			}

			if n == blockNo {
				fileSize, err := file.Seek(0, os.SEEK_END)
				if err != nil {
					log.Fatal("seek file err:", err)
					return
				}

				if fileSize < int64(offset) {
					break
				}

				_, err = file.Seek(int64(offset), os.SEEK_SET)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
			} else {
				fileSize, err := file.Seek(0, os.SEEK_END)
				if err != nil {
					log.Fatal("seek file err:", err)
					return
				}

				//TODO 这里不应该判断offset与file_size的大小
				//TODO 因为只需要从头开始处理就行了
				if fileSize < int64(offset) {
					break
				}

				_, err = file.Seek(HeaderSize, os.SEEK_SET)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
			}

			const BatchCount = 5000
			batch := &MessageBatch{messages: make([]*Message, 0, BatchCount)}
			for {
				position, err := file.Seek(0, os.SEEK_CUR)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
				msg := storage.ReadMessage(file)
				if msg == nil {
					break
				}
				msgid := storage.getMsgid(n, int(position))
				if batch.firstId == 0 {
					batch.firstId = msgid
				}

				batch.lastId = msgid
				batch.messages = append(batch.messages, msg)

				if len(batch.messages) >= BatchCount {
					c <- batch
					batch = &MessageBatch{messages: make([]*Message, 0, BatchCount)}
				}
			}
			if len(batch.messages) > 0 {
				c <- batch
			}

			n++
		}

	}()
	return c
}

func (storage *Storage) SaveIndexFileAndExit() {
	storage.flushIndex()
	os.Exit(0)
}

func (storage *Storage) flushIndex() {
	storage.mutex.Lock()
	lastId := storage.lastId
	peerIndex := storage.clonePeerIndex()
	storage.mutex.Unlock()

	storage.savePeerIndex(peerIndex)
	storage.lastSavedId = lastId
}

func (storage *Storage) FlushIndex() {
	doFlush := false
	if storage.lastId-storage.lastSavedId > 2*BlockSize {
		doFlush = true
	}
	if doFlush {
		storage.flushIndex()
	}
}

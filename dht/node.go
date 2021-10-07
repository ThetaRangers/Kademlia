package dht

import (
	"encoding/hex"
	"errors"
	"net"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/vmihailenco/msgpack"
)

type Callback func(val Packet, err error)

type CallbackChan struct {
	timer *time.Timer
	c     chan interface{}
}

type Node struct {
	contact  PacketContact
	lastSeen int64
	addr     net.Addr
	dht      *Dht
}

type StoreInst struct {
	Hash Hash
	Data []byte
}

var nonce int64 = 0

func NewPacket(dht *Dht, command Command, responseTo Hash, data isPacket_Data) Packet {
	addr, err := net.ResolveUDPAddr("udp", dht.options.ListenAddr)

	packet := Packet{
		Header: &PacketHeader{
			Nonce:       nonce,
			DateSent:    time.Now().UnixNano(),
			Command:     command,
			ResponseTo:  responseTo,
			MessageHash: []byte{},
			Sender: &PacketContact{
				Addr: addr.String(),
				Hash: dht.hash,
			},
		},
		Data: data,
	}

	nonce++

	tmp, err := msgpack.Marshal(&packet.Header)

	if err != nil {
		dht.logger.Warning(err)
	}

	packet.Header.MessageHash = NewHash(tmp)

	return packet
}

func (this *Node) newPacket(command Command, responseTo []byte, data isPacket_Data) Packet {
	return NewPacket(this.dht, command, responseTo, data)
}

func NewNodeContact(dht *Dht, addr net.Addr, contact PacketContact) *Node {
	return &Node{
		dht:      dht,
		addr:     addr,
		lastSeen: time.Now().Unix(),
		contact:  contact,
	}
}

func NewNode(dht *Dht, addr net.Addr, hash []byte) *Node {
	return NewNodeContact(dht, addr, PacketContact{
		Addr: addr.String(),
		Hash: hash,
	})
}

func (this *Node) Redacted() interface{} {
	if len(this.contact.Hash) == 0 {
		return this.contact.Addr
	}

	return hex.EncodeToString(this.contact.Hash)[:16]
}

func (this *Node) handleResponseTo(packet Packet) {
	this.dht.Lock()

	cmdQueueHash := hex.EncodeToString(packet.Header.ResponseTo)

	cb, ok := this.dht.commandQueue[cmdQueueHash]

	if !ok {
		this.dht.logger.Info(this, "x Unknown response: ", cmdQueueHash, len(this.dht.commandQueue))
		this.dht.Unlock()
		return
	}

	cb.timer.Stop()
	this.dht.Unlock()

	switch packet.Header.Command {
	case Command_NOOP:
		this.dht.logger.Debug(this, "> NOOP")
		cb.c <- packet
	case Command_PONG:
		this.OnPong(packet, cb)
	case Command_FOUND:
		this.OnFound(packet, cb)
	case Command_FOUND_NODES:
		this.OnFoundNodes(packet, cb)
	case Command_STORED:
		this.OnStored(packet, cb)
	default:
		this.dht.logger.Error(this, "x answer: UNKNOWN COMMAND", packet.Header.Command)
		return
	}

	this.dht.Lock()
	delete(this.dht.commandQueue, cmdQueueHash)
	this.dht.Unlock()
}

func (this *Node) handleRequest(packet Packet) {
	switch packet.Header.Command {
	case Command_NOOP:
	case Command_PING:
		this.OnPing(packet)
	case Command_FETCH:
		this.OnFetch(packet)
	case Command_FETCH_NODES:
		this.OnFetchNodes(packet)
	case Command_STORE:
		this.OnStore(packet)
	case Command_REPEAT_PLEASE:
		this.OnRepeatPlease(packet)
	default:
		this.dht.logger.Error(this, "x query: UNKNOWN COMMAND", packet.Header.Command)
		return
	}
}

func (this *Node) HandleInPacket(packet Packet) {
	if len(packet.Header.ResponseTo) > 0 {
		this.handleResponseTo(packet)
	} else {
		this.handleRequest(packet)
	}

}

func (this *Node) Ping() chan interface{} {
	this.dht.logger.Debug(this, "< PING")

	return this.send([]Packet{this.newPacket(Command_PING, []byte{}, nil)})
}

func (this *Node) OnPing(packet Packet) {
	this.dht.logger.Debug(this, "> PING")

	this.Pong(packet.Header.MessageHash)
}

func (this *Node) Pong(responseTo []byte) chan interface{} {
	this.dht.logger.Debug(this, "< PONG")

	return this.send([]Packet{this.newPacket(Command_PONG, responseTo, nil)})
}

func (this *Node) OnPong(packet Packet, cb CallbackChan) {
	this.dht.logger.Debug(this, "> PONG")

	cb.c <- nil
}

func (this *Node) Fetch(hash []byte) chan interface{} {
	this.dht.logger.Debug(this, "< FETCH", hash)

	data := this.newPacket(Command_FETCH, []byte{}, &Packet_Hash{Hash: hash})

	return this.send([]Packet{data})
}

func (this *Node) OnFetch(packet Packet) {
	this.dht.logger.Debug(this, "> FETCH")

	hash := packet.GetHash()

	val, ok := this.dht.store[hex.EncodeToString(hash)]

	if ok {
		this.Found(packet, hash, val)
		return
	}

	this.OnFetchNodes(packet)
}

func (this *Node) FetchNodes(hash []byte) chan interface{} {
	this.dht.logger.Debug(this, "< FETCH NODES")

	data := this.newPacket(Command_FETCH_NODES, []byte{}, &Packet_Hash{Hash: hash})

	return this.send([]Packet{data})
}

func (this *Node) OnFetchNodes(packet Packet) {
	this.dht.logger.Debug(this, "> FETCH NODES")

	hash := packet.GetHash()

	bucket := this.dht.routing.FindNode(hash)

	var nodesContact []*PacketContact

	for _, contact := range bucket {
		nodesContact = append(nodesContact, &contact)
	}

	this.FoundNodes(packet, nodesContact)
}

func (this *Node) FoundNodes(packet Packet, nodesContact []*PacketContact) {
	this.dht.logger.Debug(this, "< FOUND NODES")

	data := this.newPacket(Command_FOUND_NODES, packet.Header.MessageHash, &Packet_FoundNodes{FoundNodes: &FoundNodes{Nodes: nodesContact}})

	this.send([]Packet{data})
}

func (this *Node) OnFoundNodes(packet Packet, done CallbackChan) {
	this.dht.logger.Debug(this, "> FOUND NODES")

	done.c <- packet
}

func (this *Node) createPartMessageHeaders(hash Hash, value []byte) (res []PartHeader) {
	total := (len(value) / (BUFFER_SIZE - 128)) + 1

	i := 0
	for len(value) > 0 {
		smalest := len(value)

		if smalest > BUFFER_SIZE-128 {
			smalest = BUFFER_SIZE - 128
		}

		toSend := value[:smalest]
		value = value[smalest:]

		partHeader := PartHeader{
			Id:    int32(i),
			Total: int32(total),
			Hash:  hash,
			Data:  toSend,
		}

		i++
		res = append(res, partHeader)
	}

	return
}

func (this *Node) createFoundMessage(answerTo []byte, hash Hash, value []byte) []Packet {
	this.dht.RLock()
	msg, ok := this.dht.sentMsgs[string(hash)]
	this.dht.RUnlock()

	if ok {
		ref := updatePacketHeaderDate(msg[0].Header, answerTo)

		for i := range msg {
			msg[i].Header = &ref
		}

		return msg
	}

	found := Packet_Found{
		Found: &Found{
			Header: &PartHeader{
				Data: value,
			},
		},
	}

	pack := this.newPacket(Command_FOUND, answerTo, &found)

	parts := this.createPartMessageHeaders(hash, value)

	res := []Packet{}

	for _, part := range parts {
		found.Found.Header = &part

		pack.Data = &found

		res = append(res, pack)
	}

	this.dht.Lock()
	this.dht.sentMsgs[string(hash)] = res
	this.dht.Unlock()

	return res
}

func updatePacketHeaderDate(inHeader *PacketHeader, answerTo Hash) PacketHeader {
	header := *inHeader

	header.DateSent = time.Now().UnixNano()
	header.ResponseTo = answerTo

	nonce++

	header.Nonce = nonce

	tmp, err := msgpack.Marshal(&header)

	if err != nil {
		return PacketHeader{}
	}

	header.MessageHash = NewHash(tmp)

	return header
}

func (this *Node) createStoreMessage(hash Hash, value []byte) []Packet {
	this.dht.RLock()
	msg, ok := this.dht.sentMsgs[string(hash)]
	this.dht.RUnlock()

	if ok {
		ref := updatePacketHeaderDate(msg[0].Header, []byte{})

		for i := range msg {
			msg[i].Header = &ref
		}

		return msg
	}

	store := Packet_Store{
		Store: &Store{
			Header: &PartHeader{
				Hash: hash,
				Data: value,
			},
		},
	}

	pack := this.newPacket(Command_STORE, []byte{}, &store)

	parts := this.createPartMessageHeaders(hash, value)

	res := []Packet{}

	for _, part := range parts {
		store.Store.Header = &part

		pack.Data = &store

		res = append(res, pack)
	}

	this.dht.Lock()
	this.dht.sentMsgs[string(hash)] = res
	this.dht.Unlock()

	return res
}

func (this *Node) Found(packet Packet, hash Hash, value []byte) {
	this.dht.logger.Debug(this, "< FOUND")

	packets := this.createFoundMessage(packet.Header.MessageHash, hash, value)

	this.send(packets)
}

func (this *Node) OnFound(packet Packet, done CallbackChan) {
	this.dht.logger.Debug(this, "> FOUND")

	done.c <- packet
}

func (this *Node) Store(hash Hash, value []byte) chan interface{} {
	this.dht.logger.Debug(this, "< STORE")

	packets := this.createStoreMessage(hash, value)

	return this.send(packets)
}

func (this *Node) OnStore(packet Packet) {
	this.dht.logger.Debug(this, "> STORE")

	stInst := packet.GetStore()

	this.dht.Lock()
	_, ok := this.dht.store[hex.EncodeToString(stInst.Header.Hash)]
	this.dht.Unlock()

	itemSize := len(stInst.Header.Data)
	storageSize := this.dht.StorageSize()

	if ok ||
		!this.dht.onStore(packet) ||
		itemSize > this.dht.options.MaxItemSize ||
		itemSize+storageSize > this.dht.options.MaxStorageSize {

		this.Stored(packet, false)

		return
	}

	this.dht.Lock()
	this.dht.store[hex.EncodeToString(stInst.Header.Hash)] = stInst.Header.Data
	this.dht.Unlock()

	this.Stored(packet, true)
}

func (this *Node) Stored(packet Packet, hasStored bool) {
	this.dht.logger.Debug(this, "< STORED")

	data := this.newPacket(Command_STORED, packet.Header.MessageHash, &Packet_Ok{Ok: hasStored})

	this.send([]Packet{data})
}

func (this *Node) OnStored(packet Packet, done CallbackChan) {
	this.dht.logger.Debug(this, "> STORED")

	done.c <- packet
}

type RepeatCmd struct {
	Hash  []byte
	Value []int
}

func (this *Node) RepeatPlease(hash []byte, value []int32) chan interface{} {
	this.dht.logger.Debug(this, "< REPEAT PLEASE", len(value))

	data := this.newPacket(Command_REPEAT_PLEASE, []byte{}, &Packet_RepeatPlease{
		RepeatPlease: &RepeatPlease{
			Hash: hash,
			Data: value,
		}})

	return this.send([]Packet{data})
}

func (this *Node) OnRepeatPlease(packet Packet) {
	repeatCmd := packet.GetRepeatPlease()

	missing := repeatCmd.Data

	this.dht.logger.Debug(this, "> REPEAT PLEASE")

	hash := repeatCmd.Hash

	this.dht.Lock()

	data, ok := this.dht.sentMsgs[string(hash)]

	if !ok {
		this.dht.Unlock()
		return
	}

	cmdQueueHash := hex.EncodeToString(hash)

	cmd, ok := this.dht.commandQueue[cmdQueueHash]

	if !ok {
		this.dht.Unlock()
		return
	}

	cmd.timer.Stop()
	cmd.timer = time.NewTimer(time.Millisecond * 100)

	this.dht.logger.Debug(this, "< REPEATING")
	for _, id := range missing {
		blob, err := proto.Marshal(&data[id])

		if err != nil {
			this.dht.Unlock()
			return
		}

		_, err = this.dht.server.WriteTo(blob, this.addr)

		if err != nil {
			this.dht.Unlock()
			return
		}
	}
	this.dht.Unlock()
}

func (this *Node) send(packets []Packet) chan interface{} {
	res := make(chan interface{})

	if len(packets) == 0 {
		res <- errors.New("Nothing to send")

		return res
	}

	// TODO: check if all packets have same MessageHash

	hash := packets[0].Header.MessageHash

	timer := time.NewTimer(time.Millisecond * 100)

	cmdQueueHash := hex.EncodeToString(hash)

	this.dht.Lock()
	this.dht.commandQueue[cmdQueueHash] = CallbackChan{
		timer: timer,
		c:     res,
	}

	this.dht.Unlock()

	toSend := [][]byte{}

	for _, pack := range packets {
		blob, err := proto.Marshal(&pack)

		if err != nil {
			res <- errors.New("Error Marshal" + err.Error())

			return res
		}

		toSend = append(toSend, blob)
	}

	this.dht.Lock()
	for _, pack := range toSend {
		_, err := this.dht.server.WriteTo(pack, this.addr)

		if err != nil {
			res <- errors.New("Error Writing" + err.Error())

			this.dht.Unlock()
			return res
		}
	}
	this.dht.Unlock()

	go func() {
		<-timer.C

		this.dht.Lock()
		delete(this.dht.commandQueue, cmdQueueHash)
		this.dht.Unlock()

		var err string

		if len(this.contact.Hash) > 0 {
			err = hex.EncodeToString(this.contact.Hash[:16]) + " Timeout"
		} else {
			err = this.contact.Addr + " Timeout"
		}

		res <- errors.New(err)
	}()

	this.dht.Lock()
	defer this.dht.Unlock()
	return this.dht.commandQueue[cmdQueueHash].c
}

func (this *Node) disconnect() {
	this.dht.routing.RemoveNode(this.contact)
}

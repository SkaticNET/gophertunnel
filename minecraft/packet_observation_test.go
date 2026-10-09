package minecraft

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type observationTestPacket struct{ Value uint8 }

type multiObservationTestProtocol struct{ proto }

func (multiObservationTestProtocol) ConvertToLatest(pk packet.Packet, _ *Conn) []packet.Packet {
	return []packet.Packet{pk, &observationTestPacket{Value: 99}}
}

func (*observationTestPacket) ID() uint32 { return 0x3fe }

func (pk *observationTestPacket) Marshal(io protocol.IO) { io.Uint8(&pk.Value) }

func TestPacketObserverReportsRawAndDecodedPacket(t *testing.T) {
	var got []PacketObservation
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	conn := &Conn{
		conn:           left,
		proto:          proto{},
		pool:           packet.Pool{0x3fe: func() packet.Packet { return &observationTestPacket{} }},
		packetObserver: func(observation PacketObservation) { got = append(got, observation) },
	}
	raw := observedTestPacketBytes(t, 42)
	data, err := parseData(raw, conn)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := data.decode(conn)
	if err != nil || len(decoded) != 1 {
		t.Fatalf("decode() = (%v, %v), want one packet and no error", decoded, err)
	}
	if len(got) != 1 {
		t.Fatalf("observer calls = %d, want 1", len(got))
	}
	gotObservation := got[0]
	if gotObservation.Direction != PacketDirectionInbound || gotObservation.Ordinal != 1 || gotObservation.SubIndex != 0 {
		t.Fatalf("observer sequence = %#v", gotObservation)
	}
	if gotObservation.Header.PacketID != 0x3fe || !bytes.Equal(gotObservation.Payload, []byte{42}) {
		t.Fatalf("observer wire data = %#v, want packet ID 0x3fe and body [42]", gotObservation)
	}
	if gotObservation.LocalAddr == nil || gotObservation.RemoteAddr == nil {
		t.Fatal("observer omitted connection addresses needed for private correlation")
	}
	packet, ok := gotObservation.Packet.(*observationTestPacket)
	if !ok || packet.Value != 42 || gotObservation.DecodeError {
		t.Fatalf("observer decoded packet = %#v, want value 42 without decode error", gotObservation)
	}
}

func TestPacketObserverReportsDecodeFailureAndAdvancesOrdinal(t *testing.T) {
	var got []PacketObservation
	conn := &Conn{
		proto:          proto{},
		pool:           packet.Pool{0x3fe: func() packet.Packet { return &observationTestPacket{} }},
		packetObserver: func(observation PacketObservation) { got = append(got, observation) },
	}
	data, err := parseData(observedTestPacketBytes(t), conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.decode(conn); err == nil {
		t.Fatal("decode() error = nil, want truncated-packet error")
	}
	if len(got) != 1 || !got[0].DecodeError || got[0].Stage != PacketObservationDecodeError || got[0].Ordinal != 1 || !bytes.Equal(got[0].Payload, nil) {
		t.Fatalf("decode failure observation = %#v", got)
	}
	data, err = parseData(observedTestPacketBytes(t, 42), conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.decode(conn); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Ordinal != 2 || got[1].DecodeError {
		t.Fatalf("second observation = %#v, want ordinal 2 success", got)
	}
}

func TestPacketObserverReportsSubIndexForAdditionalPackets(t *testing.T) {
	var got []PacketObservation
	conn := &Conn{
		proto:          multiObservationTestProtocol{},
		pool:           packet.Pool{0x3fe: func() packet.Packet { return &observationTestPacket{} }},
		packets:        make(chan *packetData, 1),
		additional:     make(chan queuedPacket, 16),
		ctx:            context.Background(),
		packetObserver: func(observation PacketObservation) { got = append(got, observation) },
	}
	data, err := parseData(observedTestPacketBytes(t, 42), conn)
	if err != nil {
		t.Fatal(err)
	}
	conn.packets <- data
	first, err := conn.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	firstMetadata := conn.LastPacketMetadata()
	second, err := conn.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	secondMetadata := conn.LastPacketMetadata()
	if first == nil || second == nil || firstMetadata.Ordinal != 1 || firstMetadata.SubIndex != 0 || secondMetadata.Ordinal != 1 || secondMetadata.SubIndex != 1 {
		t.Fatalf("returned packet metadata = (%#v, %#v), want ordinal 1 sub-indexes 0 and 1", firstMetadata, secondMetadata)
	}
	if len(got) != 2 || got[0].Ordinal != 1 || got[0].SubIndex != 0 || got[1].Ordinal != 1 || got[1].SubIndex != 1 {
		t.Fatalf("observations = %#v, want ordinal 1 sub-indexes 0 and 1", got)
	}
}

func TestPacketObserverPanicsDoNotBreakDecode(t *testing.T) {
	conn := &Conn{
		proto:          proto{},
		pool:           packet.Pool{0x3fe: func() packet.Packet { return &observationTestPacket{} }},
		packetObserver: func(PacketObservation) { panic("observer failure") },
	}
	data, err := parseData(observedTestPacketBytes(t, 42), conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.decode(conn); err != nil {
		t.Fatalf("decode() error = %v after observer panic", err)
	}
}

func TestPacketObserverDistinguishesTrailingBytes(t *testing.T) {
	var got []PacketObservation
	conn := &Conn{
		proto:          proto{},
		pool:           packet.Pool{0x3fe: func() packet.Packet { return &observationTestPacket{} }},
		packetObserver: func(observation PacketObservation) { got = append(got, observation) },
	}
	data, err := parseData(observedTestPacketBytes(t, 42, 43), conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.decode(conn); err == nil {
		t.Fatal("decode() error = nil, want trailing bytes")
	}
	if len(got) != 1 || got[0].Stage != PacketObservationTrailingBytes || got[0].TrailingBytes != 1 {
		t.Fatalf("trailing-byte observation = %#v", got)
	}
}

func observedTestPacketBytes(t *testing.T, body ...byte) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := (&packet.Header{PacketID: 0x3fe}).Write(&b); err != nil {
		t.Fatal(err)
	}
	_, _ = b.Write(body)
	return b.Bytes()
}

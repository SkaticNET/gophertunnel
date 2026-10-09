package minecraft

import (
	"net"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// PacketDirection identifies the direction of a packet relative to a Conn.
type PacketDirection string

const (
	PacketDirectionInbound  PacketDirection = "inbound"
	PacketDirectionOutbound PacketDirection = "outbound"
)

// PacketObservationStage distinguishes wire parsing, packet decoding, and
// outbound encoding from transport delivery.
type PacketObservationStage string

const (
	PacketObservationHeaderError   PacketObservationStage = "packet_header_error"
	PacketObservationDecoded       PacketObservationStage = "decoded"
	PacketObservationDecodeError   PacketObservationStage = "decode_error"
	PacketObservationUnknownID     PacketObservationStage = "unknown_packet_id"
	PacketObservationTrailingBytes PacketObservationStage = "trailing_bytes"
	PacketObservationEncodedQueued PacketObservationStage = "encoded_queued"
)

// PacketMetadata links a returned packet to its raw packet observation without
// exposing the packet body through Conn.LastPacketMetadata.
type PacketMetadata struct {
	Direction PacketDirection
	Ordinal   uint64
	SubIndex  uint32
	PacketID  uint32
}

// PacketObservation reports packet bytes before decode and the decode result.
// Payload is valid only for the duration of the callback and must not be changed.
type PacketObservation struct {
	Direction     PacketDirection
	LocalAddr     net.Addr
	RemoteAddr    net.Addr
	Ordinal       uint64
	SubIndex      uint32
	Header        packet.Header
	Payload       []byte
	Packet        packet.Packet
	DecodeError   bool
	Stage         PacketObservationStage
	TrailingBytes uint32
}

// PacketObserver receives packet observations synchronously on the connection
// read or write path. It should return quickly and must not mutate Payload.
// Panics in an observer are recovered so diagnostics cannot break a Conn.
type PacketObserver func(PacketObservation)

type queuedPacket struct {
	packet   packet.Packet
	metadata PacketMetadata
}

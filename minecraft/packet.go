package minecraft

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// packetData holds the data of a Minecraft packet.
type packetData struct {
	h          *packet.Header
	full       []byte
	rawPayload []byte
	payload    *bytes.Buffer
	ordinal    uint64
}

func (p *packetData) metadata(subIndex uint32) PacketMetadata {
	metadata := PacketMetadata{Direction: PacketDirectionInbound, Ordinal: p.ordinal, SubIndex: subIndex}
	if p.h != nil {
		metadata.PacketID = p.h.PacketID
	}
	return metadata
}

// parseData parses the packet data slice passed into a packetData struct.
func parseData(data []byte, conn *Conn) (*packetData, error) {
	buf := bytes.NewBuffer(data)
	header := &packet.Header{}
	ordinal := conn.inboundPacketOrdinal.Add(1)
	if err := header.Read(buf); err != nil {
		// We don't return this as an error as it's not in the hand of the user to control this. Instead,
		// we return to reading a new packet.
		local, remote := conn.packetAddresses()
		conn.observePacket(PacketObservation{
			Direction: PacketDirectionInbound, Ordinal: ordinal, Payload: data,
			LocalAddr: local, RemoteAddr: remote,
			DecodeError: true, Stage: PacketObservationHeaderError,
		})
		return nil, fmt.Errorf("read packet header: %w", err)
	}
	if conn.packetFunc != nil {
		// The packet func was set, so we call it.
		conn.packetFunc(*header, buf.Bytes(), conn.RemoteAddr(), conn.LocalAddr())
	}
	return &packetData{h: header, full: data, rawPayload: buf.Bytes(), payload: buf, ordinal: ordinal}, nil
}

type unknownPacketError struct {
	id uint32
}

func (err unknownPacketError) Error() string {
	return fmt.Sprintf("unexpected packet (ID=%v)", err.id)
}

// decode decodes the packet payload held in the packetData and returns the packet.Packet decoded.
func (p *packetData) decode(conn *Conn) (pks []packet.Packet, err error) {
	// Attempt to fetch the packet with the right packet ID from the pool.
	pkFunc, ok := conn.pool[p.h.PacketID]
	var pk packet.Packet
	var failureStage PacketObservationStage
	var trailingBytes uint32
	local, remote := conn.packetAddresses()
	defer func() {
		if recoveredErr := recover(); recoveredErr != nil {
			err = fmt.Errorf("decode packet %T: %w", pk, recoveredErr.(error))
		}
		if err != nil {
			if failureStage == "" {
				failureStage = PacketObservationDecodeError
			}
			conn.observePacket(PacketObservation{
				Direction: PacketDirectionInbound, Ordinal: p.ordinal, Header: *p.h,
				LocalAddr: local, RemoteAddr: remote,
				Payload: p.rawPayload, DecodeError: true, Stage: failureStage, TrailingBytes: trailingBytes,
			})
			if !errors.Is(err, unknownPacketError{}) && conn.disconnectOnInvalidPacket {
				_ = conn.Close()
			}
			return
		}
		for i, decoded := range pks {
			conn.observePacket(PacketObservation{
				Direction: PacketDirectionInbound, Ordinal: p.ordinal, SubIndex: uint32(i),
				LocalAddr: local, RemoteAddr: remote,
				Header: *p.h, Payload: p.rawPayload, Packet: decoded, Stage: PacketObservationDecoded,
			})
		}
	}()
	if !ok {
		// No packet with the ID. This may be a custom packet of some sorts.
		pk = &packet.Unknown{PacketID: p.h.PacketID}
		if conn.disconnectOnUnknownPacket {
			failureStage = PacketObservationUnknownID
			return nil, unknownPacketError{id: p.h.PacketID}
		}
	} else {
		pk = pkFunc()
	}

	r := conn.proto.NewReader(p.payload, conn.shieldID.Load(), conn.readerLimits)
	pk.Marshal(r)
	if p.payload.Len() != 0 {
		failureStage = PacketObservationTrailingBytes
		trailingBytes = uint32(p.payload.Len())
		err = fmt.Errorf("decode packet %T: %v unread bytes left: 0x%x", pk, p.payload.Len(), p.payload.Bytes())
	}
	if conn.disconnectOnInvalidPacket && err != nil {
		return nil, err
	}
	return conn.proto.ConvertToLatest(pk, conn), err
}

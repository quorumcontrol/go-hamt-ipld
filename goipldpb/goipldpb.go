package goipldpb

import (
	"fmt"
	"strings"

	ptypes "github.com/gogo/protobuf/types"

	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"

	"github.com/gogo/protobuf/proto"
)

const googleApis = "type.googleapis.com/"

var ErrUnimplemented = fmt.Errorf("unimplemented")

func WrapObject(msg proto.Message) (format.Node, error) {
	any, err := marshalAny(msg)
	if err != nil {
		return nil, err
	}

	data, err := any.Marshal()
	if err != nil {
		return nil, err
	}

	return merkledag.NodeWithData(data), nil
}

func DecodeInto(bits []byte, out proto.Message) error {
	protonode, err := merkledag.DecodeProtobuf(bits)
	if err != nil {
		return err
	}
	any := new(ptypes.Any)
	if err := any.Unmarshal(protonode.Data()); err != nil {
		return err
	}
	return unmarshalAny(any, out)
}

// UnmarshalAny parses the protocol buffer representation in a google.protobuf.Any
// message and places the decoded result in pb. It returns an error if type of
// contents of Any message does not match type of pb message.
//
// pb can be a proto.Message, or a *DynamicAny.
func unmarshalAny(any *ptypes.Any, pb proto.Message) error {
	unmarshaler, ok := pb.(proto.Unmarshaler)
	if !ok {
		return fmt.Errorf("message must support unmarshal")
	}
	aname, err := anyMessageName(any)
	if err != nil {
		return err
	}

	mname := proto.MessageName(pb)
	if aname != mname {
		return fmt.Errorf("mismatched message type: got %q want %q", aname, mname)
	}
	return unmarshaler.Unmarshal(any.Value)
}

// MarshalAny takes the protocol buffer and encodes it into google.protobuf.Any.
func marshalAny(pb proto.Message) (*ptypes.Any, error) {
	marshaler, ok := pb.(proto.Marshaler)
	if !ok {
		return nil, fmt.Errorf("proto message must support Marshal")
	}
	value, err := marshaler.Marshal()
	if err != nil {
		return nil, err
	}
	return &ptypes.Any{TypeUrl: googleApis + proto.MessageName(pb), Value: value}, nil
}

// AnyMessageName returns the name of the message contained in a google.protobuf.Any message.
//
// Note that regular type assertions should be done using the Is
// function. AnyMessageName is provided for less common use cases like filtering a
// sequence of Any messages based on a set of allowed message type names.
func anyMessageName(any *ptypes.Any) (string, error) {
	if any == nil {
		return "", fmt.Errorf("message is nil")
	}
	slash := strings.LastIndex(any.TypeUrl, "/")
	if slash < 0 {
		return "", fmt.Errorf("message type url %q is invalid", any.TypeUrl)
	}
	return any.TypeUrl[slash+1:], nil
}

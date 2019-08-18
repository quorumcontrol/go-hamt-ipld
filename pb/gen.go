//go:generate protoc -I=. -I=${GOPATH}/src/github.com/gogo/protobuf/protobuf -I=${GOPATH}/src --gogoslick_out==Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,paths=source_relative:. hamt.proto

package pb

package codec

import (
	"fmt"

	"github.com/fagongzi/goetty"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/filesyncer/pkg/pb"
)

var (
	baseCodec = &codec{}
	syncCodec = &goetty.SyncCodec{}
	// FileDecoder file decoder
	FileDecoder = goetty.NewIntLengthFieldBasedDecoder(baseCodec)
	// FileEncoder file encoder
	FileEncoder = goetty.NewIntLengthFieldBasedEncoder(baseCodec)
	// SyncDecoder sync decoder
	SyncDecoder = goetty.NewIntLengthFieldBasedDecoder(syncCodec)
	// SyncEncoder sync encoder
	SyncEncoder = goetty.NewIntLengthFieldBasedEncoder(syncCodec)
)

type codec struct {
}

func (codec *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	in.MarkedBytesReaded()

	var value pbutil.PB
	cmd := pb.Cmd(data[0])

	switch cmd {
	case pb.CmdUploadInit:
		value = &pb.InitUploadReq{}
	case pb.CmdUploadInitRsp:
		value = &pb.InitUploadRsp{}
	case pb.CmdUpload:
		value = &pb.UploadReq{}
	case pb.CmdUploadRsp:
		value = &pb.UploadRsp{}
	case pb.CmdUploadComplete:
		value = &pb.UploadCompleteReq{}
	case pb.CmdUploadCompleteRsp:
		value = &pb.UploadCompleteRsp{}
	case pb.CmdUploadContinue:
		value = &pb.UploadContinue{}
	case pb.CmdHB:
		value = &pb.Heartbeat{}
	case pb.CmdSysUsage:
		value = &pb.SysUsage{}
	}

	if value != nil {
		err := value.Unmarshal(data[1:])
		if err != nil {
			return false, nil, err
		}
		return true, value, nil
	}

	return false, nil, fmt.Errorf("not support cmd: %d", cmd)
}

// Encode encode
func (codec *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	var value pbutil.PB
	var size int
	var cmd byte

	if msg, ok := data.(*pb.InitUploadReq); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadInit)
	} else if msg, ok := data.(*pb.InitUploadRsp); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadInitRsp)
	} else if msg, ok := data.(*pb.UploadReq); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUpload)
	} else if msg, ok := data.(*pb.UploadRsp); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadRsp)
	} else if msg, ok := data.(*pb.UploadCompleteReq); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadComplete)
	} else if msg, ok := data.(*pb.UploadCompleteRsp); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadCompleteRsp)
	} else if msg, ok := data.(*pb.UploadContinue); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdUploadContinue)
	} else if msg, ok := data.(*pb.Heartbeat); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdHB)
	} else if msg, ok := data.(*pb.SysUsage); ok {
		value = msg
		size = msg.Size()
		cmd = byte(pb.CmdSysUsage)
	}

	if value != nil {
		out.WriteByte(cmd)
		out.Expansion(size)
		idx := out.GetWriteIndex()
		_, err := value.MarshalTo(out.RawBuf()[idx : idx+size])
		out.SetWriterIndex(idx + size)
		return err
	}

	return fmt.Errorf("not support value: %T,%+v", data, data)
}

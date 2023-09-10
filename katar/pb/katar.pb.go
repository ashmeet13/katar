// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.4
// source: katar.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SuccessResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *SuccessResponse) Reset() {
	*x = SuccessResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_katar_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SuccessResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SuccessResponse) ProtoMessage() {}

func (x *SuccessResponse) ProtoReflect() protoreflect.Message {
	mi := &file_katar_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SuccessResponse.ProtoReflect.Descriptor instead.
func (*SuccessResponse) Descriptor() ([]byte, []int) {
	return file_katar_proto_rawDescGZIP(), []int{0}
}

type FailureResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Reason string `protobuf:"bytes,1,opt,name=Reason,proto3" json:"Reason,omitempty"`
}

func (x *FailureResponse) Reset() {
	*x = FailureResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_katar_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FailureResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FailureResponse) ProtoMessage() {}

func (x *FailureResponse) ProtoReflect() protoreflect.Message {
	mi := &file_katar_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FailureResponse.ProtoReflect.Descriptor instead.
func (*FailureResponse) Descriptor() ([]byte, []int) {
	return file_katar_proto_rawDescGZIP(), []int{1}
}

func (x *FailureResponse) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

type PublishRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic   string `protobuf:"bytes,1,opt,name=Topic,proto3" json:"Topic,omitempty"`
	Payload []byte `protobuf:"bytes,2,opt,name=Payload,proto3" json:"Payload,omitempty"`
}

func (x *PublishRequest) Reset() {
	*x = PublishRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_katar_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishRequest) ProtoMessage() {}

func (x *PublishRequest) ProtoReflect() protoreflect.Message {
	mi := &file_katar_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishRequest.ProtoReflect.Descriptor instead.
func (*PublishRequest) Descriptor() ([]byte, []int) {
	return file_katar_proto_rawDescGZIP(), []int{2}
}

func (x *PublishRequest) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *PublishRequest) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

type PublishResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Response:
	//
	//	*PublishResponse_Success
	//	*PublishResponse_Failure
	Response isPublishResponse_Response `protobuf_oneof:"Response"`
}

func (x *PublishResponse) Reset() {
	*x = PublishResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_katar_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishResponse) ProtoMessage() {}

func (x *PublishResponse) ProtoReflect() protoreflect.Message {
	mi := &file_katar_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishResponse.ProtoReflect.Descriptor instead.
func (*PublishResponse) Descriptor() ([]byte, []int) {
	return file_katar_proto_rawDescGZIP(), []int{3}
}

func (m *PublishResponse) GetResponse() isPublishResponse_Response {
	if m != nil {
		return m.Response
	}
	return nil
}

func (x *PublishResponse) GetSuccess() *SuccessResponse {
	if x, ok := x.GetResponse().(*PublishResponse_Success); ok {
		return x.Success
	}
	return nil
}

func (x *PublishResponse) GetFailure() *FailureResponse {
	if x, ok := x.GetResponse().(*PublishResponse_Failure); ok {
		return x.Failure
	}
	return nil
}

type isPublishResponse_Response interface {
	isPublishResponse_Response()
}

type PublishResponse_Success struct {
	Success *SuccessResponse `protobuf:"bytes,1,opt,name=Success,proto3,oneof"`
}

type PublishResponse_Failure struct {
	Failure *FailureResponse `protobuf:"bytes,2,opt,name=Failure,proto3,oneof"`
}

func (*PublishResponse_Success) isPublishResponse_Response() {}

func (*PublishResponse_Failure) isPublishResponse_Response() {}

var File_katar_proto protoreflect.FileDescriptor

var file_katar_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x6b, 0x61, 0x74, 0x61, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x6b,
	0x61, 0x74, 0x61, 0x72, 0x22, 0x11, 0x0a, 0x0f, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x29, 0x0a, 0x0f, 0x46, 0x61, 0x69, 0x6c, 0x75,
	0x72, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x52, 0x65,
	0x61, 0x73, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x52, 0x65, 0x61, 0x73,
	0x6f, 0x6e, 0x22, 0x40, 0x0a, 0x0e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x18, 0x0a, 0x07, 0x50, 0x61,
	0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x50, 0x61, 0x79,
	0x6c, 0x6f, 0x61, 0x64, 0x22, 0x85, 0x01, 0x0a, 0x0f, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63,
	0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x6b, 0x61, 0x74, 0x61,
	0x72, 0x2e, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x48, 0x00, 0x52, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x32, 0x0a, 0x07,
	0x46, 0x61, 0x69, 0x6c, 0x75, 0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e,
	0x6b, 0x61, 0x74, 0x61, 0x72, 0x2e, 0x46, 0x61, 0x69, 0x6c, 0x75, 0x72, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x00, 0x52, 0x07, 0x46, 0x61, 0x69, 0x6c, 0x75, 0x72, 0x65,
	0x42, 0x0a, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0x43, 0x0a, 0x05,
	0x4b, 0x61, 0x74, 0x61, 0x72, 0x12, 0x3a, 0x0a, 0x07, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68,
	0x12, 0x15, 0x2e, 0x6b, 0x61, 0x74, 0x61, 0x72, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x16, 0x2e, 0x6b, 0x61, 0x74, 0x61, 0x72, 0x2e,
	0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x42, 0x25, 0x5a, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x61, 0x73, 0x68, 0x6d, 0x65, 0x65, 0x74, 0x31, 0x33, 0x2f, 0x6b, 0x61, 0x74, 0x61, 0x72, 0x2f,
	0x6b, 0x61, 0x74, 0x61, 0x72, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_katar_proto_rawDescOnce sync.Once
	file_katar_proto_rawDescData = file_katar_proto_rawDesc
)

func file_katar_proto_rawDescGZIP() []byte {
	file_katar_proto_rawDescOnce.Do(func() {
		file_katar_proto_rawDescData = protoimpl.X.CompressGZIP(file_katar_proto_rawDescData)
	})
	return file_katar_proto_rawDescData
}

var file_katar_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_katar_proto_goTypes = []interface{}{
	(*SuccessResponse)(nil), // 0: katar.SuccessResponse
	(*FailureResponse)(nil), // 1: katar.FailureResponse
	(*PublishRequest)(nil),  // 2: katar.PublishRequest
	(*PublishResponse)(nil), // 3: katar.PublishResponse
}
var file_katar_proto_depIdxs = []int32{
	0, // 0: katar.PublishResponse.Success:type_name -> katar.SuccessResponse
	1, // 1: katar.PublishResponse.Failure:type_name -> katar.FailureResponse
	2, // 2: katar.Katar.Publish:input_type -> katar.PublishRequest
	3, // 3: katar.Katar.Publish:output_type -> katar.PublishResponse
	3, // [3:4] is the sub-list for method output_type
	2, // [2:3] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_katar_proto_init() }
func file_katar_proto_init() {
	if File_katar_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_katar_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SuccessResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_katar_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FailureResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_katar_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_katar_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_katar_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*PublishResponse_Success)(nil),
		(*PublishResponse_Failure)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_katar_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_katar_proto_goTypes,
		DependencyIndexes: file_katar_proto_depIdxs,
		MessageInfos:      file_katar_proto_msgTypes,
	}.Build()
	File_katar_proto = out.File
	file_katar_proto_rawDesc = nil
	file_katar_proto_goTypes = nil
	file_katar_proto_depIdxs = nil
}

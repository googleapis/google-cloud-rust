package protobuf

import (
	"log/slog"

	"github.com/googleapis/google-cloud-rust/generator/src/genclient"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
)

func parseHTTPInfo(m proto.Message) *genclient.HTTPInfo {
	eHTTP := proto.GetExtension(m, annotations.E_Http)
	httpRule := eHTTP.(*annotations.HttpRule)
	var info *genclient.HTTPInfo
	switch httpRule.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		info = &genclient.HTTPInfo{
			Method:  "GET",
			RawPath: httpRule.GetGet(),
		}
	case *annotations.HttpRule_Post:
		info = &genclient.HTTPInfo{
			Method:  "POST",
			RawPath: httpRule.GetPost(),
		}
	case *annotations.HttpRule_Put:
		info = &genclient.HTTPInfo{
			Method:  "PUT",
			RawPath: httpRule.GetPut(),
		}
	case *annotations.HttpRule_Delete:
		info = &genclient.HTTPInfo{
			Method:  "DELETE",
			RawPath: httpRule.GetDelete(),
		}
	case *annotations.HttpRule_Patch:
		info = &genclient.HTTPInfo{
			Method:  "PATCH",
			RawPath: httpRule.GetPatch(),
		}
	default:
		slog.Warn("unsupported http method", "method", httpRule.GetPattern())
	}
	if info != nil {
		info.Body = httpRule.GetBody()
	}
	return info
}

func parseDefaultHost(m proto.Message) string {
	eDefaultHost := proto.GetExtension(m, annotations.E_DefaultHost)
	defaultHost := eDefaultHost.(string)
	if defaultHost == "" {
		slog.Warn("missing default host for service", "service", m.ProtoReflect().Descriptor().FullName())
	}
	return defaultHost
}

// TODO(codyoss): The following:
// - proto.GetExtension(m, annotations.E_FieldBehavior)
// - proto.GetExtension(m, annotations.E_FieldInfo)
// - proto.GetExtension(m, extendedops.E_OperationService) -- and all other associated annotations
// - proto.GetExtension(m, longrunning.E_OperationInfo)
// - proto.GetExtension(m, annotations.E_OauthScopes)
// - proto.GetExtension(m, annotations.E_Routing)
// - proto.GetExtension(m, annotations.E_ApiVersion)

package ws_test

import (
	"testing"

	"github.com/mezon/mezon-go-sdk-turbo/lib/ws"
)

func BenchmarkBuildContentPlain(b *testing.B) {
	text := "Chào bạn! Mình sẵn sàng hỗ trợ."
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ws.BuildContent(text)
	}
}

func BenchmarkBuildContentRich(b *testing.B) {
	text := "**Nguồn:** https://meknow.mezon.vn/references/abc\nChạy `uv sync` trước."
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ws.BuildContent(text)
	}
}

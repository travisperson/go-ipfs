package epictest

import (
	"testing"
	core_testutil "github.com/jbenet/go-ipfs/core/testutil"
)

func benchmarkAddCat(numBytes int64, conf core_testutil.LatencyConfig, b *testing.B) {

	b.StopTimer()
	b.SetBytes(numBytes)
	data := RandomBytes(numBytes) // we don't want to measure the time it takes to generate this data
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		if err := DirectAddCat(data, conf); err != nil {
			b.Fatal(err)
		}
	}
}

var instant = core_testutil.LatencyConfig{}.All_Instantaneous()

func BenchmarkInstantaneousAddCat1KB(b *testing.B)   { benchmarkAddCat(1*KB, instant, b) }
func BenchmarkInstantaneousAddCat1MB(b *testing.B)   { benchmarkAddCat(1*MB, instant, b) }
func BenchmarkInstantaneousAddCat2MB(b *testing.B)   { benchmarkAddCat(2*MB, instant, b) }
func BenchmarkInstantaneousAddCat4MB(b *testing.B)   { benchmarkAddCat(4*MB, instant, b) }
func BenchmarkInstantaneousAddCat8MB(b *testing.B)   { benchmarkAddCat(8*MB, instant, b) }
func BenchmarkInstantaneousAddCat16MB(b *testing.B)  { benchmarkAddCat(16*MB, instant, b) }
func BenchmarkInstantaneousAddCat32MB(b *testing.B)  { benchmarkAddCat(32*MB, instant, b) }
func BenchmarkInstantaneousAddCat64MB(b *testing.B)  { benchmarkAddCat(64*MB, instant, b) }
func BenchmarkInstantaneousAddCat128MB(b *testing.B) { benchmarkAddCat(128*MB, instant, b) }
func BenchmarkInstantaneousAddCat256MB(b *testing.B) { benchmarkAddCat(256*MB, instant, b) }

var routing = core_testutil.LatencyConfig{}.Routing_Slow()

func BenchmarkRoutingSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*MB, routing, b) }
func BenchmarkRoutingSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*MB, routing, b) }
func BenchmarkRoutingSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*MB, routing, b) }
func BenchmarkRoutingSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*MB, routing, b) }
func BenchmarkRoutingSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*MB, routing, b) }
func BenchmarkRoutingSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*MB, routing, b) }
func BenchmarkRoutingSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*MB, routing, b) }
func BenchmarkRoutingSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*MB, routing, b) }
func BenchmarkRoutingSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*MB, routing, b) }
func BenchmarkRoutingSlowAddCat512MB(b *testing.B) { benchmarkAddCat(512*MB, routing, b) }

var network = core_testutil.LatencyConfig{}.Network_NYtoSF()

func BenchmarkNetworkSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*MB, network, b) }
func BenchmarkNetworkSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*MB, network, b) }
func BenchmarkNetworkSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*MB, network, b) }
func BenchmarkNetworkSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*MB, network, b) }
func BenchmarkNetworkSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*MB, network, b) }
func BenchmarkNetworkSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*MB, network, b) }
func BenchmarkNetworkSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*MB, network, b) }
func BenchmarkNetworkSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*MB, network, b) }
func BenchmarkNetworkSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*MB, network, b) }

var hdd = core_testutil.LatencyConfig{}.Blockstore_7200RPM()

func BenchmarkBlockstoreSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*MB, hdd, b) }

var mixed = core_testutil.LatencyConfig{}.Network_NYtoSF().Blockstore_SlowSSD2014().Routing_Slow()

func BenchmarkMixedAddCat1MBXX(b *testing.B) { benchmarkAddCat(1*MB, mixed, b) }
func BenchmarkMixedAddCat2MBXX(b *testing.B) { benchmarkAddCat(2*MB, mixed, b) }
func BenchmarkMixedAddCat4MBXX(b *testing.B) { benchmarkAddCat(4*MB, mixed, b) }
func BenchmarkMixedAddCat8MBXX(b *testing.B) { benchmarkAddCat(8*MB, mixed, b) }
func BenchmarkMixedAddCat16MBX(b *testing.B) { benchmarkAddCat(16*MB, mixed, b) }
func BenchmarkMixedAddCat32MBX(b *testing.B) { benchmarkAddCat(32*MB, mixed, b) }
func BenchmarkMixedAddCat64MBX(b *testing.B) { benchmarkAddCat(64*MB, mixed, b) }
func BenchmarkMixedAddCat128MB(b *testing.B) { benchmarkAddCat(128*MB, mixed, b) }
func BenchmarkMixedAddCat256MB(b *testing.B) { benchmarkAddCat(256*MB, mixed, b) }

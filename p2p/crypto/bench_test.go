package crypto

import "testing"

const (
	numBytes = 1 // byte
)

func BenchmarkSign(b *testing.B) {
	secret, _, err := GenerateKeyPair(RSA, 1024)
	if err != nil {
		b.Fatal(err)
	}
	someData := make([]byte, numBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := secret.Sign(someData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerify(b *testing.B) {
	secret, public, err := GenerateKeyPair(RSA, 1024)
	if err != nil {
		b.Fatal(err)
	}
	someData := make([]byte, numBytes)
	signature, err := secret.Sign(someData)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valid, err := public.Verify(someData, signature)
		if err != nil {
			b.Fatal(err)
		}
		if !valid {
			b.Fatal("signature should be valid")
		}
	}
}

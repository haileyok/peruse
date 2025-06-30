package peruse

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"time"

	atcrypto "github.com/bluesky-social/indigo/atproto/crypto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/golang-jwt/jwt/v5"
)

func (s *Server) getKeyForDid(ctx context.Context, did syntax.DID) (crypto.PublicKey, error) {
	ident, err := s.directory.LookupDID(ctx, did)
	if err != nil {
		return nil, err
	}

	return ident.PublicKey()
}

func (s *Server) fetchKeyFunc(ctx context.Context) func(tok *jwt.Token) (any, error) {
	return func(tok *jwt.Token) (any, error) {
		issuer, ok := tok.Claims.(jwt.MapClaims)["iss"].(string)
		if !ok {
			return nil, fmt.Errorf("missing 'iss' field from auth header JWT")
		}
		did, err := syntax.ParseDID(issuer)
		if err != nil {
			return nil, fmt.Errorf("invalid DID in 'iss' field from auth header JWT")
		}

		val, ok := s.keyCache.Get(did.String())
		if ok {
			return val, nil
		}

		k, err := s.getKeyForDid(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("failed to look up public key for DID (%q): %w", did, err)
		}
		s.keyCache.Add(did.String(), k)
		return k, nil
	}
}

func (s *Server) checkJwt(ctx context.Context, tok string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	return s.checkJwtConfig(ctx, tok)
}

func (s *Server) checkJwtConfig(ctx context.Context, tok string, config ...jwt.ParserOption) (string, error) {
	validMethods := []string{SigningMethodES256K.Alg(), SigningMethodES256.Alg()}
	config = append(config, jwt.WithValidMethods(validMethods))
	p := jwt.NewParser(config...)
	t, err := p.Parse(tok, s.fetchKeyFunc(ctx))
	if err != nil {
		return "", fmt.Errorf("failed to parse auth header jwt: %w", err)
	}

	clms, ok := t.Claims.(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("invalid token claims")
	}

	did, ok := clms["iss"].(string)
	if !ok {
		return "", fmt.Errorf("no issuer present in returned claims")
	}

	return did, nil
}

// copied from Jaz's https://github.com/ericvolp12/jwt-go-secp256k1

var (
	SigningMethodES256K *SigningMethodAtproto
	SigningMethodES256  *SigningMethodAtproto
)

// implementation of jwt.SigningMethod.
type SigningMethodAtproto struct {
	alg      string
	hash     crypto.Hash
	toOutSig toOutSig
	sigLen   int
}

type toOutSig func(sig []byte) []byte

func init() {
	SigningMethodES256K = &SigningMethodAtproto{
		alg:      "ES256K",
		hash:     crypto.SHA256,
		toOutSig: toES256K,
		sigLen:   64,
	}
	jwt.RegisterSigningMethod(SigningMethodES256K.Alg(), func() jwt.SigningMethod {
		return SigningMethodES256K
	})
	SigningMethodES256 = &SigningMethodAtproto{
		alg:      "ES256",
		hash:     crypto.SHA256,
		toOutSig: toES256,
		sigLen:   64,
	}
	jwt.RegisterSigningMethod(SigningMethodES256.Alg(), func() jwt.SigningMethod {
		return SigningMethodES256
	})
}

// Errors returned on different problems.
var (
	ErrWrongKeyFormat  = errors.New("wrong key type")
	ErrBadSignature    = errors.New("bad signature")
	ErrVerification    = errors.New("signature verification failed")
	ErrFailedSigning   = errors.New("failed generating signature")
	ErrHashUnavailable = errors.New("hasher unavailable")
)

func (sm *SigningMethodAtproto) Verify(signingString string, sig []byte, key interface{}) error {
	pub, ok := key.(atcrypto.PublicKey)
	if !ok {
		return ErrWrongKeyFormat
	}

	if !sm.hash.Available() {
		return ErrHashUnavailable
	}

	if len(sig) != sm.sigLen {
		return ErrBadSignature
	}

	return pub.HashAndVerifyLenient([]byte(signingString), sig)
}

func (sm *SigningMethodAtproto) Sign(signingString string, key interface{}) ([]byte, error) {
	return nil, ErrFailedSigning
}

func (sm *SigningMethodAtproto) Alg() string {
	return sm.alg
}

func toES256K(sig []byte) []byte {
	return sig[:64]
}

func toES256(sig []byte) []byte {
	return sig[:64]
}

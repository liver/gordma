package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"errors"
	"fmt"
)

type ProtectDomain struct {
	pd       *C.struct_ibv_pd
	isClosed bool
}

func NewProtectDomain(ctx *RdmaContext) (*ProtectDomain, error) {
	pd, err := C.ibv_alloc_pd(ctx.ctx)
	if err != nil {
		return nil, err
	}
	return &ProtectDomain{
		pd: pd,
	}, err
}

func (p *ProtectDomain) Close() error {
	if p.isClosed {
		return fmt.Errorf("PD is already closed")
	}
	errno := C.ibv_dealloc_pd(p.pd)
	if errno != 0 {
		return errors.New("failed to dealloc PD")
	}
	p.pd = nil
	p.isClosed = true
	return nil
}

package pb

import (
	fmt "fmt"

	"github.com/ipfs/go-cid"
)

func (p *Pointer) Link() cid.Cid {
	if len(p.LinkBits) == 0 {
		return cid.Undef
	}
	c, err := cid.Cast(p.LinkBits)
	if err != nil {
		fmt.Println("unknown CID")
		return cid.Undef
	}
	return c
}

func (p *Pointer) SetLink(c cid.Cid) {
	p.LinkBits = c.Bytes()
}
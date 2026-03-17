package share

import (
	"fmt"
)

// NodeRole định nghĩa vai trò của node trong RDA network
// Tương ứng với ρ (rho) trong bài báo
type NodeRole uint8

const (
	// ClientNode (ρ = 0): Node thường join chỉ 1 hàng + 1 cột
	ClientNode NodeRole = 0
	// BootstrapNode (ρ = 1): Node join TẤT CẢ hàng (0...R-1) + 1 cột
	// Là xương sống mạng để ngăn chặn phân mảnh
	BootstrapNode NodeRole = 1
)

// String trả về string representation của NodeRole
func (r NodeRole) String() string {
	switch r {
	case ClientNode:
		return "ClientNode(ρ=0)"
	case BootstrapNode:
		return "BootstrapNode(ρ=1)"
	default:
		return fmt.Sprintf("Unknown(%d)", r)
	}
}

// IsValid kiểm tra xem NodeRole có hợp lệ không
func (r NodeRole) IsValid() bool {
	return r == ClientNode || r == BootstrapNode
}

// ParseNodeRole phân tích string thành NodeRole
func ParseNodeRole(s string) (NodeRole, error) {
	switch s {
	case "client", "ClientNode", "0":
		return ClientNode, nil
	case "bootstrap", "BootstrapNode", "1":
		return BootstrapNode, nil
	default:
		return ClientNode, fmt.Errorf("invalid node role: %s", s)
	}
}

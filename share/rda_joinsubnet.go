package share

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

var joinSubnetLog = logging.Logger("rda.joinsubnet")

// JoinSubnetRequest đại diện cho một yêu cầu JOINSUBNET
type JoinSubnetRequest struct {
	NodeID    string   // Peer ID của node
	NodeRole  NodeRole // Client(0) hay Bootstrap(1)
	MyRow     int      // Hàng của node
	MyCol     int      // Cột của node
	GridDims  GridDimensions
	RequestID string
}

// JoinSubnetResult kết quả của JOINSUBNET
type JoinSubnetResult struct {
	Success            bool
	RowsJoined         []int
	ColsJoined         []int
	TotalSubnetsJoined int
	LatencyMs          int64
	Error              string
}

// RDAJoinSubnet thực hiện hoạt động JOINSUBNET theo yêu cầu bài báo
type RDAJoinSubnet struct {
	pubsub       *pubsub.PubSub
	gridManager  *RDAGridManager
	nodeID       peer.ID
	role         NodeRole
	myRow        int
	myCol        int
	gridDims     GridDimensions
	mu           sync.RWMutex
	joinedTopics map[string]interface{}
}

// NewRDAJoinSubnet tạo một JoinSubnet handler mới
func NewRDAJoinSubnet(
	ps *pubsub.PubSub,
	gridManager *RDAGridManager,
	nodeID peer.ID,
	role NodeRole,
	myRow, myCol int,
) *RDAJoinSubnet {
	return &RDAJoinSubnet{
		pubsub:       ps,
		gridManager:  gridManager,
		nodeID:       nodeID,
		role:         role,
		myRow:        myRow,
		myCol:        myCol,
		gridDims:     gridManager.GetGridDimensions(),
		joinedTopics: make(map[string]interface{}),
	}
}

// Execute thực hiện JOINSUBNET operation
func (j *RDAJoinSubnet) Execute(ctx context.Context) (*JoinSubnetResult, error) {
	reqID := fmt.Sprintf("req-%d-%d", time.Now().UnixNano()/1e6, 0)
	startTime := time.Now()

	result := &JoinSubnetResult{
		RowsJoined: []int{},
		ColsJoined: []int{},
		Error:      "",
	}

	joinSubnetLog.Infof(
		"[%s] JOINSUBNET START - role=%s, position=(%d,%d), grid=%dx%d",
		reqID, j.role, j.myRow, j.myCol, j.gridDims.Rows, j.gridDims.Cols,
	)

	var err error
	switch j.role {
	case ClientNode:
		err = j.joinAsClient(ctx, reqID, result)
	case BootstrapNode:
		err = j.joinAsBootstrap(ctx, reqID, result)
	default:
		err = fmt.Errorf("invalid node role: %d", j.role)
	}

	result.LatencyMs = time.Since(startTime).Milliseconds()
	result.TotalSubnetsJoined = len(result.RowsJoined) + len(result.ColsJoined)

	if err != nil {
		result.Success = false
		result.Error = err.Error()
		joinSubnetLog.Warnf(
			"[%s] JOINSUBNET FAILED - role=%s: %v (latency=%dms)",
			reqID, j.role, err, result.LatencyMs,
		)
		return result, err
	}

	result.Success = true
	joinSubnetLog.Infof(
		"[%s] JOINSUBNET SUCCESS - role=%s, joined=%d (rows=%v, cols=%v), latency=%dms ✓",
		reqID, j.role, result.TotalSubnetsJoined, result.RowsJoined, result.ColsJoined, result.LatencyMs,
	)

	return result, nil
}

// joinAsClient thực hiện JOINSUBNET cho ClientNode
// Yêu cầu: Join đúng 1 hàng + 1 cột
func (j *RDAJoinSubnet) joinAsClient(ctx context.Context, reqID string, result *JoinSubnetResult) error {
	joinSubnetLog.Debugf("[%s] ClientNode: joining row=%d, col=%d", reqID, j.myRow, j.myCol)

	// Join hàng của node
	rowTopic := fmt.Sprintf("rda/row/%d", j.myRow)
	joinSubnetLog.Debugf("[%s] ClientNode: attempting to join row topic: %s", reqID, rowTopic)

	if err := j.joinTopic(ctx, rowTopic); err != nil {
		return fmt.Errorf("failed to join row topic %s: %w", rowTopic, err)
	}
	result.RowsJoined = append(result.RowsJoined, j.myRow)
	joinSubnetLog.Infof("[%s] ClientNode: joined row subnet: %s ✓", reqID, rowTopic)

	// Join cột của node
	colTopic := fmt.Sprintf("rda/col/%d", j.myCol)
	joinSubnetLog.Debugf("[%s] ClientNode: attempting to join col topic: %s", reqID, colTopic)

	if err := j.joinTopic(ctx, colTopic); err != nil {
		return fmt.Errorf("failed to join col topic %s: %w", colTopic, err)
	}
	result.ColsJoined = append(result.ColsJoined, j.myCol)
	joinSubnetLog.Infof("[%s] ClientNode: joined col subnet: %s ✓", reqID, colTopic)

	joinSubnetLog.Infof(
		"[%s] ClientNode COMPLETE - joined 2 subnets (row=%d, col=%d)",
		reqID, j.myRow, j.myCol,
	)

	return nil
}

// joinAsBootstrap thực hiện JOINSUBNET cho BootstrapNode
// Yêu cầu: Join TẤT CẢ hàng (0...R-1) + 1 cột
// Đây là xương sống mạng để ngăn phân mảnh
func (j *RDAJoinSubnet) joinAsBootstrap(ctx context.Context, reqID string, result *JoinSubnetResult) error {
	joinSubnetLog.Infof(
		"[%s] BootstrapNode: joining ALL rows (0 to %d) + column %d",
		reqID, j.gridDims.Rows-1, j.myCol,
	)

	// PHASE 1: Join TẤT CẢ hàng (CRITICAL)
	joinSubnetLog.Infof(
		"[%s] BootstrapNode PHASE 1: joining %d row topics...",
		reqID, j.gridDims.Rows,
	)

	successCount := 0
	failedRows := []int{}

	for row := 0; row < int(j.gridDims.Rows); row++ {
		rowTopic := fmt.Sprintf("rda/row/%d", row)

		if err := j.joinTopic(ctx, rowTopic); err != nil {
			joinSubnetLog.Warnf(
				"[%s] BootstrapNode: failed to join row %d topic %s: %v",
				reqID, row, rowTopic, err,
			)
			failedRows = append(failedRows, row)
			continue
		}

		result.RowsJoined = append(result.RowsJoined, row)
		successCount++

		// Log progress mỗi 32 hàng
		if (row+1)%32 == 0 {
			joinSubnetLog.Debugf(
				"[%s] BootstrapNode PHASE 1: progress %d/%d rows joined",
				reqID, successCount, j.gridDims.Rows,
			)
		}
	}

	if successCount == 0 {
		return fmt.Errorf(
			"BootstrapNode failed: could not join any row topics (0/%d)",
			int(j.gridDims.Rows),
		)
	}

	joinSubnetLog.Infof(
		"[%s] BootstrapNode PHASE 1 COMPLETE - joined %d/%d rows ✓",
		reqID, successCount, j.gridDims.Rows,
	)

	if len(failedRows) > 0 {
		joinSubnetLog.Warnf(
			"[%s] BootstrapNode: WARNING - %d rows failed: %v",
			reqID, len(failedRows), failedRows,
		)
	}

	// PHASE 2: Join cột của bootstrap node
	joinSubnetLog.Debugf("[%s] BootstrapNode PHASE 2: joining column topic", reqID)

	colTopic := fmt.Sprintf("rda/col/%d", j.myCol)
	joinSubnetLog.Debugf("[%s] BootstrapNode: attempting to join col topic: %s", reqID, colTopic)

	if err := j.joinTopic(ctx, colTopic); err != nil {
		return fmt.Errorf("failed to join col topic %s: %w", colTopic, err)
	}
	result.ColsJoined = append(result.ColsJoined, j.myCol)
	joinSubnetLog.Infof("[%s] BootstrapNode: joined col subnet: %s ✓", reqID, colTopic)

	totalSubnets := len(result.RowsJoined) + len(result.ColsJoined)
	joinSubnetLog.Infof(
		"[%s] BootstrapNode COMPLETE - joined %d subnets (rows=%d, col=%d) ✓",
		reqID, totalSubnets, len(result.RowsJoined), j.myCol,
	)

	return nil
}

// joinTopic tham gia vào một topic GossipSub
func (j *RDAJoinSubnet) joinTopic(ctx context.Context, topic string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Check nếu đã join rồi
	if _, exists := j.joinedTopics[topic]; exists {
		return nil
	}

	// Join topic
	topicHandle, err := j.pubsub.Join(topic)
	if err != nil {
		return fmt.Errorf("failed to join topic %s: %w", topic, err)
	}

	// Subscribe để nhận tin
	sub, err := topicHandle.Subscribe()
	if err != nil {
		topicHandle.Close()
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	// Lưu lại topic handle
	j.joinedTopics[topic] = topicHandle

	// Close subscription (sẽ subscribe lại nếu cần)
	sub.Cancel()

	return nil
}

// GetJoinedSubnets trả về danh sách subnets đã tham gia
func (j *RDAJoinSubnet) GetJoinedSubnets() (rows, cols []int) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	rowsCopy := make([]int, 0)
	colsCopy := make([]int, 0)

	for topic := range j.joinedTopics {
		var row int
		_, err := fmt.Sscanf(topic, "rda/row/%d", &row)
		if err == nil {
			rowsCopy = append(rowsCopy, row)
			continue
		}

		var col int
		_, err = fmt.Sscanf(topic, "rda/col/%d", &col)
		if err == nil {
			colsCopy = append(colsCopy, col)
		}
	}

	return rowsCopy, colsCopy
}

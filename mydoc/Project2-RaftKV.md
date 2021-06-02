## Part B

[UML](./imgs/RaftKV.vsdx)

### 如何驱动Raft？
tinykv 的 Raft 复用自 etcd，采用逻辑时钟，需要由上层模块进行驱动。

[How to drive Raft?](./imgs/how_to_drive_raft.vsdx)

### Implement Raft ready process

#### `proposeRaftCommand()`的逻辑
`proposeRaftCommand()`的调用者有两个，分别是`peerMsgHandler`的`HandleMsg()`和`onRaftGCLogTick()`。先看`HandleMsg`：

```go
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	...
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	...
}
```

`MsgTypeRaftCmd`类型的消息来自`RaftstoreRouter.SendRaftCommand`：
```go
func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}
```

这个调用来自上层客户端。以测试代码中的 Put 操作为例，调用链为：
```
    (*Cluster).MustPutCF
             |
             V
    (*Cluster).Request
             |
             V
    (*Cluster).CallCommandOnLeader
             |
             V
    (*Cluster).CallCommand
             |
             V
    (*NodeSimulator).CallCommandOnStore
             |
             V
    (*RaftstoreRouter).SendRaftCommand
```

对于来自客户端的请求，处理思路是将其封装成 Raft 日志放进 Raft group 进行同步，当 Raft 把日志提交上来后就可以执行请求中的操作了（这是`HandleRaftReady`要实现的功能）。

```go
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	p := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, p)
	} else {
		d.proposeData(msg, p)
	}
}
```

首先要构造一个`proposal`。`nextProposalIndex()`返回的是底层`Raftlog`的`LastIndex + 1`，也就是接下来要放进 Raft group 进行同步的最新一条日志的 index。`Callback`是与客户端沟通的桥梁：
```go
type Callback struct {
	Resp *raft_cmdpb.RaftCmdResponse
	Txn  *badger.Txn // used for GetSnap
	done chan struct{}
}
```
客户端发出请求后会在`done`上面阻塞等待，当我们把请求处理完毕后向调用`Callback.Done()`向`done`写入一个`struct{}{}`即可将结果通知给客户端。

构造好的`proposal` `p`会被保存到`proposals`里面。**`proposals`是`peer`的成员变量，在 tinykv 提供的代码中这是一个`[]*proposal`类型，我不知道写 tinykv 的人为什么这么愚蠢，所以我把它改成了`map[uint64]*proposal`类型，key 就是`p.index`。如果用数组的话，查找的时候只能顺序查找。**

`proposeData()`追加的日志在 Raft group 中达成一致后会被提交上来，`HandleRaftReady()`通过`Ready()`获得 committed entries 后调用`process()`进行处理。我们需要根据不同的消息类型实现不同的处理逻辑。Get/Put/Delete/Snap 是以 CmdType 发送过来的，针对 KV 数据，调用`processRequest()`；CompactLog 等是通过 AdminCmdType 发送过来的，调用`processAdminRequest()`。处理完毕后调用`notify()`将结果通知给客户端。

```go
func (d *peerMsgHandler) notify(entry *eraftpb.Entry, fn func(p *proposal)) {
	p, ok := d.proposals[entry.Index]
	if !ok {
		return
	}
	if p.index != entry.Index {
		log.Warnf("no matched proposal found for entry [index = %d, term = %d]", entry.Index, entry.Term)
		return
	}

	if !d.IsLeader() {
		log.Warnf("%x is not a leader", d.PeerId())
		NotifyNotLeader(d.regionId, entry.Term, p.cb)
		d.proposals = make(map[uint64]*proposal)
		return
	}
	if p.term == entry.Term {
		fn(p)
	} else {
		log.Infof("%x proposal term not matched at index = %d [entry.term = %d, proposal.term = %d]",
			d.PeerId(), entry.Index, entry.Term, p.term)
		NotifyStaleReq(entry.Term, p.cb)
	}
	delete(d.proposals, p.index)
}
```
关于`notify()`的几点说明：

1. 客户端的请求是在 Leader 上发出的，但是这期间 Leader 可能会挂掉。如果调用`notify()`时`peer`不再是 Leader，那么就要给上层返回一个错误并清空`proposals`。上层如果发现 Leader 变更，就会继续尝试新的 Leader，参见`(*Cluster).CallCommandOnLeader`。
2. 如果在`proposals`中找到一个`proposal` `p`，`p.index == entry.Index`但是`p.term != entry.Term`，这说明之前调用`proposeData()`扔给 Raft 的那条日志被丢弃了，在相同 index 上面出现了一条 term 更大的日志。原因可能是以前那个 Leader 挂了，新的 Leader 用更新的日志把旧的日志覆盖了。同样地，我们要给上层返回一个错误。

文档里有这么一段说明：
> In this stage, you may consider these errors, and others will be processed in project3:
>
> - ErrNotLeader: the raft command is proposed on a follower. so use it to let the client > try other peers.
> - ErrStaleCommand: It may due to leader changes that some logs are not committed and overrided with new leaders’ logs. But the client doesn’t know that and is still waiting for the response. So you should return this to let the client knows and retries the command again.

`Cluster.Request()`：
```go
func (c *Cluster) Request(key []byte, reqs []*raft_cmdpb.Request, timeout time.Duration) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	startTime := time.Now()
	for i := 0; i < 10 || time.Now().Sub(startTime) < timeout; i++ {
		region := c.GetRegion(key)
		regionID := region.GetId()
		req := NewRequest(regionID, region.RegionEpoch, reqs)
		resp, txn := c.CallCommandOnLeader(&req, timeout)
		if resp == nil {
			// it should be timeouted innerly
			SleepMS(100)
			continue
		}
		if resp.Header.Error != nil {
			SleepMS(100)
			continue
		}
		return resp, txn
	}
	panic("request timeout")
}
```

可以看到这里并未严格区分错误类型，只要返回错误就会一直重试，直到超时。但为了规范性，`notify()`仍对错误类型进行了区分。

#### `HandleRaftReady()`的逻辑
在[如何驱动Raft？](#如何驱动Raft？)已经分析过了。
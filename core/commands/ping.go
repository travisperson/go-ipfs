package commands

import (
	"time"

	cmds "github.com/jbenet/go-ipfs/commands"
	"github.com/jbenet/go-ipfs/net/message"
	peer "github.com/jbenet/go-ipfs/peer"

	context "github.com/jbenet/go-ipfs/Godeps/_workspace/src/code.google.com/p/go.net/context"
)

var PingCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "send echo request packets to IPFS hosts",
		Synopsis: `
ipfs ping <peer.ID> - Send pings to a peer using the routing system to discover its address
`,
		ShortDescription: `
ipfs ping is a tool to find a node (in the routing system),
send pings, wait for pongs, and print out round-trip latency information. 
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("peer-id", true, true, "ID of peer to ping"),
	},
	Run: func(req cmds.Request) (interface{}, error) {
		n, err := req.Context().GetNode()
		if err != nil {
			return nil, err
		}

		if !n.OnlineMode() {
			return nil, errNotOnline
		}

		peerID := peer.DecodePrettyID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")
		const kPingTimeout = 10 * time.Second
		ctx, _ := context.WithTimeout(context.Background(), kPingTimeout)
		p, err := n.Routing.FindPeer(ctx, peerID)
		if err != nil {
			return nil, err
		}
		if err := n.Network.DialPeer(ctx, p); err != nil {
			return nil, err
		}
		msg := message.New(p, nil)
		n.Network.SendMessage(msg)
		return nil, nil
	},
}

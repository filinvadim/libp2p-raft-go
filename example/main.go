package main

import (
	"bytes"
	"context"
	go_crypto "crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	raft "github.com/filinvadim/libp2p-raft-go"
	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/crypto/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	_ = golog.SetLogLevel(raft.RaftLogSystemName, "panic")
	_ = golog.SetLogLevel("basichost", "panic")
}

// must be static and deterministic addresses
var eachNodeStaticAddresses = []raft.ServerID{
	"/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWKpZ9HB956cbo9XqDqvgAXFJRZnxbWErK9CRrkD9Am1Zi",
	"/ip4/127.0.0.1/tcp/4002/p2p/12D3KooWJX7egPDVhKxHuo96CtTQ15S3j3PYy3kS1nfyfV3uHsY9",
	"/ip4/127.0.0.1/tcp/4003/p2p/12D3KooWMLedr8JPBPptjc9wRRq4G3FhJjJ7Hd6Jfc4BMa9zSsoH",
}

func rubLibp2pNode(port int) (host.Host, error) {
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)

	// generate stable static PK for deterministic peer ID
	pk, err := GenerateKeyFromSeed([]byte(listenAddr))
	if err != nil {
		return nil, err
	}

	// cast PK to LibP2P PK format
	p2pPrivKey, err := crypto.UnmarshalEd25519PrivateKey(pk)
	if err != nil {
		return nil, err
	}

	node, err := libp2p.New(
		libp2p.Identity(p2pPrivKey),
		libp2p.ListenAddrStrings(listenAddr),
	)
	if err != nil {
		return nil, err
	}

	fmt.Println()
	fmt.Printf("node %s: listening on %s\n", node.ID().String(), listenAddr)
	fmt.Println()

	return node, nil
}

func runRaftNode(node host.Host, isInitiator bool, addrInfos []peer.AddrInfo) (*raft.ConsensusService, error) {
	config := &raft.Config{
		IsClusterInitiator: isInitiator,
		BootstrapNodes:     addrInfos,
	}

	raftNode, err := raft.NewLibP2pRaft(context.Background(), config)
	if err != nil {
		return nil, err
	}
	if err := raftNode.Start(node); err != nil {
		return nil, err
	}

	return raftNode, nil
}

func main() {
	var interruptChan = make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT)

	// use helper to convert bootstrap nodes addresses to peer.AddrInfo types
	addrInfos, err := ServerIDsToAddrInfos(eachNodeStaticAddresses)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("bootstrap node addresses:", addrInfos)

	p2pNode1, err := rubLibp2pNode(4001)
	if err != nil {
		log.Println("error:", err)
		return
	}
	defer p2pNode1.Close()

	p2pNode2, err := rubLibp2pNode(4002)
	if err != nil {
		log.Println("error:", err)
		return
	}
	defer p2pNode2.Close()

	p2pNode3, err := rubLibp2pNode(4003)
	if err != nil {
		log.Println("error:", err)
		return
	}
	defer p2pNode3.Close()

	// here connect each node to each other because we haven't turned on discovery
	for _, n := range []host.Host{p2pNode1, p2pNode2, p2pNode3} {
		for _, nn := range []host.Host{p2pNode1, p2pNode2, p2pNode3} {
			if n.ID() == nn.ID() {
				continue
			}
			if err := n.Connect(context.Background(), peer.AddrInfo{ID: nn.ID(), Addrs: nn.Addrs()}); err != nil {
				log.Println("failed to connect to node:", n.ID(), "=>>>", nn.ID(), err)
			} else {
				log.Println("node", n.ID(), "connected to", nn.ID())
			}
		}
	}

	var raftNode1, raftNode2, raftNode3 *raft.ConsensusService
	// each raft node waits sync to each other so run them in parallel
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() (err error) {
		// this node initiates Raft bootstrap cluster - only a single node can do that
		raftNode1, err = runRaftNode(p2pNode1, true, addrInfos)
		return err
	})
	g.Go(func() (err error) {
		time.Sleep(time.Second * 3) // it's not necessary but makes run smooth
		raftNode2, err = runRaftNode(p2pNode2, true, addrInfos)
		return err
	})
	g.Go(func() (err error) {
		time.Sleep(time.Second * 5) // it's not necessary but makes run smooth
		raftNode3, err = runRaftNode(p2pNode3, true, addrInfos)
		return err
	})

	waitErr := g.Wait() // waiting all raft nodes successful run
	defer raftNode1.Shutdown()
	defer raftNode2.Shutdown()
	defer raftNode3.Shutdown()
	if waitErr != nil {
		log.Println("wait error:", waitErr)
		return
	}

	fmt.Println()
	fmt.Println("all raft nodes started successfully")
	fmt.Println()

	// find leader which has power to add new voters to the cluster
	for i, n := range []*raft.ConsensusService{raftNode1, raftNode2, raftNode3} {
		if n.LeaderID() == n.ID() { // hi, I'm a leader node!
			// leader adds other voters
			n.AddVoter(raftNode1.ID())
			n.AddVoter(raftNode2.ID())
			n.AddVoter(raftNode3.ID())
			// leader commits new state - only leader can do that
			newState, err := n.CommitState(map[string]string{"hello": "world"})
			if err != nil {
				log.Printf("leader node %d: commit state error: %v\n", i, err)
			} else {
				fmt.Printf("leader node %d: committed new state: %v\n", i, newState)
			}
		}
	}

	newState1, err := raftNode1.CurrentState()
	if err != nil {
		log.Println("node 1: commit state error:", err)
		return
	}
	fmt.Println("node 1: current state:", newState1) // check new state

	for {
		// waiting until other Raft nodes sync their states
		time.Sleep(time.Second)

		select {
		case <-interruptChan:
			return
		default:
			newState2, err := raftNode2.CurrentState()
			if err != nil || newState2 == nil {
				log.Println("node 2: get state error:", err)
				continue
			}
			state2Len := len(*newState2)

			newState3, err := raftNode3.CurrentState()
			if err != nil || newState2 == nil {
				log.Println("node 3: get state error:", err)
				continue
			}
			state3Len := len(*newState3)
			// is it still empty?
			if state2Len != 0 && state3Len != 0 {
				fmt.Println("node2 and node3 state:", newState2, newState3)
				fmt.Println()
				fmt.Println("following nodes updated state successfully!")
				return
			}
		}
	}
}

// GenerateKeyFromSeed is a helper function that allows to create deterministic peer ID
func GenerateKeyFromSeed(seed []byte) (ed25519.PrivateKey, error) {
	if len(seed) == 0 {
		return nil, errors.New("seed is empty")
	}
	hashAlgo := go_crypto.SHA256
	keyType := pb.KeyType_Ed25519
	seed = append(seed, uint8(hashAlgo))
	seed = append(seed, uint8(keyType))
	hash := sha256.Sum256(seed)
	privKey, _, err := crypto.GenerateEd25519Key(bytes.NewReader(hash[:]))
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}
	return privKey.Raw()
}

// ServerIDsToAddrInfos just a converter betweet Raft and LibP2P formats
func ServerIDsToAddrInfos(bootstrapNodes []raft.ServerID) (infos []peer.AddrInfo, err error) {
	for _, addr := range bootstrapNodes {
		maddr, err := multiaddr.NewMultiaddr(string(addr))
		if err != nil {
			return nil, err
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, err
		}
		infos = append(infos, *addrInfo)
	}
	return infos, nil
}

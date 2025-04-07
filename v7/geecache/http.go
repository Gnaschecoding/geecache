package geecache

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"v7/geecache/consistenthash"
	"v7/geecache/geecachepb"
)

var (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// HTTPPool implements PeerPicker for a pool of HTTP peers.

type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self        string
	basePath    string
	mux         sync.Mutex // guards peers and httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// NewHTTPPool initializes an HTTP pool of peers.
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (p *HTTPPool) Log(format string, args ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, args...))
}

// ServeHTTP handle all http requests
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// /<basepath>/<groupname>/<key> required
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&geecachepb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	//找到数据了
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}

// Set updates the pool's list of peers.
func (p *HTTPPool) Set(peers ...string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...) //这里面的peers切片  就是 peer 的值为 http://192.168.1.100，这代表一个远程节点的地址。
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

var _ PeerPicker = (*HTTPPool)(nil) //判断httpGetter是否实现了 PeerGetter
// PickPeer picks a peer according to key
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()

	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

type httpGetter struct {
	baseURL string
}

// 判断是否实现了 PeerGetter
var _ PeerGetter = (*httpGetter)(nil)

func (g *httpGetter) Get(in *geecachepb.Request, out *geecachepb.Response) error {
	//例如，若 group 的值为 hello world，key 的值为 user&admin，不进行转义的话，
	//构建的 URL 可能是 http://example.com?group=hello world&key=user&admin，
	//服务器会把 hello 和 world 以及 user 和 admin 分别当作不同的参数值，从而产生解析错误。
	//经过转义后，group 变为 hello+world，key 变为 user%26admin，
	//构建的 URL 为 http://example.com?group=hello+world&key=user%26admin，
	//服务器就能正确解析参数了。
	u := fmt.Sprintf(
		"%v%v/%v",
		g.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)

	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	if err := proto.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

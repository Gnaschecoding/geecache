package geecache

import (
	"net"
	"strconv"
)

func validPeerAddr(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	// 校验端口是数字并且在合法范围内
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return false
	}

	// 允许 localhost
	if host == "localhost" {
		return true
	}

	// 校验是否是合法 IP 地址
	ip := net.ParseIP(host)
	return ip != nil
}
